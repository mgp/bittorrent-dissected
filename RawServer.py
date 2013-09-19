# Written by Bram Cohen
# see LICENSE.txt for license information

from bisect import insort
import socket
from cStringIO import StringIO
from traceback import print_exc
from errno import EWOULDBLOCK, ENOBUFS
try:
    from select import poll, error, POLLIN, POLLOUT, POLLERR, POLLHUP
    timemult = 1000
except ImportError:
    from selectpoll import poll, error, POLLIN, POLLOUT, POLLERR, POLLHUP
    timemult = 1
from threading import Thread, Event
from time import time, sleep
import sys
from random import randrange

all = POLLIN | POLLOUT

class SingleSocket:
    def __init__(self, raw_server, sock, handler):
        # The RawServer instance.
        self.raw_server = raw_server
        # The socket object.
        self.socket = sock
        # The Connection from Encrypter.
        self.handler = handler
        # Bytes enqueued for sending.
        self.buffer = []
        # The last time we read data from the socket.
        self.last_hit = time()
        # The FD for the socket.
        self.fileno = sock.fileno()
        # Whether this connection has been established, or ready for reading or writing.
        self.connected = False
        
    def get_ip(self):
        try:
            return self.socket.getpeername()[0]
        except socket.error:
            return 'no connection'
        
    def close(self):
        # Release resources for garbage collection.
        sock = self.socket
        self.socket = None
        self.buffer = []
        del self.raw_server.single_sockets[self.fileno]
        # Unregister the socket FD from those passed to select.
        self.raw_server.poll.unregister(sock)
        # Close the socket.
        sock.close()

    def shutdown(self, val):
        # Enables one or both halves of the connection.
        self.socket.shutdown(val)

    def is_flushed(self):
        # Whether all enqueud data was written to the socket's underlying send buffer.
        return len(self.buffer) == 0

    def write(self, s):
        assert self.socket is not None
        # Enqueue the data and then immediately try to write it.
        self.buffer.append(s)
        if len(self.buffer) == 1:
            self.try_write()

    def try_write(self):
        if self.connected:
            # Only try to write if still connected.
            try:
                while self.buffer != []:
                    # Write data to the socket buffer until we see backpressure.
                    amount = self.socket.send(self.buffer[0])
                    if amount != len(self.buffer[0]):
                        # Could not write all data to the socket buffer.
                        if amount != 0:
                            # Remove from our buffer what is now in the socket buffer.
                            self.buffer[0] = self.buffer[0][amount:]
                        # Attempt to write more on the next loop of the reactor.
                        break
                    # Wrote all data to the socket buffer.
                    del self.buffer[0]
            except socket.error, e:
                code, msg = e
                if code != EWOULDBLOCK:
                    # Error is not because the socket buffer is full.
                    self.raw_server.dead_from_write.append(self)
                    return
        if self.buffer == []:
            # No more data to write, so only register FD for reading.
            self.raw_server.poll.register(self.socket, POLLIN)
        else:
            # Still data to write, so register FD for reading and writing.
            self.raw_server.poll.register(self.socket, all)


def default_error_handler(x):
    print x


class RawServer:
    def __init__(self, doneflag, timeout_check_interval, timeout, noisy = True,
            errorfunc = default_error_handler, maxconnects = 55):
        # The time in seconds between monitoring sockets for timeout, or no keepalives.
        self.timeout_check_interval = timeout_check_interval
        # The timeout in seconds.
        self.timeout = timeout
        # The socket polling implementation.
        self.poll = poll()
        # {socket: SingleSocket}
        # Maps each socket FD to its SingleSocket instance.
        self.single_sockets = {}
        # SingleSocket instances that failed on writing and are waiting to be closed.
        self.dead_from_write = []
        # Threading Event object to terminate the reactor loop.
        self.doneflag = doneflag
        # Flag controlling extra debugging info if a run function raises an exception.
        self.noisy = noisy
        # Callback invoked with a string describing any error.
        self.errorfunc = errorfunc
        # The maximum connections to maintain; any new connections are closed after this.
        self.maxconnects = maxconnects
        # Scheduled tasks consisting of (time, func) pairs.
        self.funcs = []
        # Unscheduled tasks consisting of (func, delay) pairs.
        self.unscheduled_tasks = []
        self.add_task(self.scan_for_timeouts, timeout_check_interval)

    def add_task(self, func, delay):
        self.unscheduled_tasks.append((func, delay))

    def scan_for_timeouts(self):
        # Run this function again after timeout_check_interval seconds.
        self.add_task(self.scan_for_timeouts, self.timeout_check_interval)
        # Compute the timeout threshold.
        t = time() - self.timeout
        # Gather all SingleSocket instances that last sent data before this threshold.
        tokill = []
        for s in self.single_sockets.values():
            if s.last_hit < t:
                tokill.append(s)
        # Close these sockets that have timed out.
        for k in tokill:
            if k.socket is not None:
                self._close_socket(k)

    def bind(self, port, bind = '', reuse = False):
        self.bindaddr = bind
        # Create the socket to listen for incoming connections on.
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if reuse:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(0)
        try:
            server.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 32)
        except:
            pass
        # Bind it to the given port.
        server.bind((bind, port))
        # Register with the polling implementation so ready for listening.
        server.listen(5)
        self.poll.register(server, POLLIN)
        self.server = server

    def start_connection(self, dns, handler = None):
        if handler is None:
            # Use the handler of this server if one is not provided.
            handler = self.handler
        # Create a socket to connect through.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        sock.bind((self.bindaddr, 0))
        try:
            # Start connecting.
            sock.connect_ex(dns)
        except socket.error:
            raise
        except Exception, e:
            raise socket.error(str(e))
        # Reading data will finish establishing the connection.
        self.poll.register(sock, POLLIN)
        # Map from the socket FD to a SingleSocket instance containing it.
        s = SingleSocket(self, sock, handler)
        self.single_sockets[sock.fileno()] = s
        # Return the SingleSocket instance.
        return s
        
    def handle_events(self, events):
        for sock, event in events:
            if sock == self.server.fileno():
                # This is the socket we're listening for new connections on.
                if event & (POLLHUP | POLLERR) != 0:
                    # There was an error listening.
                    self.poll.unregister(self.server)
                    self.server.close()
                    self.errorfunc('lost server socket')
                else:
                    # There must be a peer connecting.
                    try:
                        # Get the socket to the peer.
                        newsock, addr = self.server.accept()
                        newsock.setblocking(0)
                        if len(self.single_sockets) >= self.maxconnects:
                            # We already have the maximum number of connections.
                            # So close it immediately.
                            newsock.close()
                            continue
                        # Map from the socket FD to a SingleSocket instance containing it.
                        nss = SingleSocket(self, newsock, self.handler)
                        self.single_sockets[newsock.fileno()] = nss
                        # Register the new connection with the polling implementation.
                        self.poll.register(newsock, POLLIN)
                        # Notify the Connection object from Encrypter.
                        self.handler.external_connection_made(nss)
                    except socket.error:
                        sleep(1)
            else:
                s = self.single_sockets.get(sock)
                if s is None:
                    # We don't know of this socket.
                    continue
                # If this socket was still connecting, then it's now connected.
                s.connected = True
                if (event & (POLLHUP | POLLERR)) != 0:
                    # Close the socket on an error.
                    self._close_socket(s)
                    continue
                if (event & POLLIN) != 0:
                    try:
                        # Update the last time we read data from this socket.
                        s.last_hit = time()
                        # Read all the data we can.
                        data = s.socket.recv(100000)
                        if data == '':
                            # Read EOF, so close the socket.
                            self._close_socket(s)
                        else:
                            # Notify the Connection object from Encrypter.
                            s.handler.data_came_in(s, data)
                    except socket.error, e:
                        code, msg = e
                        if code != EWOULDBLOCK:
                            # Error is not because the socket buffer is full.
                            self._close_socket(s)
                            continue
                if (event & POLLOUT) != 0 and s.socket is not None and not s.is_flushed():
                    # Write more enqueued data to the socket.
                    s.try_write()
                    if s.is_flushed():
                        # Flushed the connection, notify the Connection object from Encrypter.
                        s.handler.connection_flushed(s)

    def pop_unscheduled(self):
        try:
            # Schedule each unscheduled task.
            while True:
                (func, delay) = self.unscheduled_tasks.pop()
                insort(self.funcs, (time() + delay, func))
        except IndexError:
            pass

    def listen_forever(self, handler):
        self.handler = handler
        try:
            while not self.doneflag.isSet():
                try:
                    # Schedule each unscheduled task.
                    self.pop_unscheduled()
                    # Poll, while sleeping until the time of the next scheduled task.
                    if len(self.funcs) == 0:
                        period = 2 ** 30
                    else:
                        period = self.funcs[0][0] - time()
                    if period < 0:
                        period = 0
                    events = self.poll.poll(period * timemult)
                    if self.doneflag.isSet():
                        return
                    while len(self.funcs) > 0 and self.funcs[0][0] <= time():
                        # Pop and then run the next scheduled task to run now.
                        garbage, func = self.funcs[0]
                        del self.funcs[0]
                        try:
                            func()
                        except KeyboardInterrupt:
                            print_exc()
                            return
                        except:
                            if self.noisy:
                                # If it failed, log extra debugging info.
                                data = StringIO()
                                print_exc(file = data)
                                self.errorfunc(data.getvalue())
                    # Close the dead sockets so handle_events doesn't process them.
                    self._close_dead()
                    # Read or write to each socket returned by polling.
                    self.handle_events(events)
                    if self.doneflag.isSet():
                        return
                    # Close the dead sockets so tasks on the next iteration them.
                    self._close_dead()
                except error, e:
                    if self.doneflag.isSet():
                        return
                    # I can't find a coherent explanation for what the behavior should be here,
                    # and people report conflicting behavior, so I'll just try all the possibilities
                    try:
                        code, msg, desc = e
                    except:
                        try:
                            code, msg = e
                        except:
                            code = ENOBUFS
                    if code == ENOBUFS:
                        self.errorfunc("Have to exit due to the TCP stack flaking out")
                        return
                except KeyboardInterrupt:
                    print_exc()
                    return
                except:
                    data = StringIO()
                    print_exc(file = data)
                    self.errorfunc(data.getvalue())
        finally:
            # Terminating the program. Close all connections to peers.
            for ss in self.single_sockets.values():
                ss.close()
            # Close the socket that listens for incoming connections.
            self.server.close()

    def _close_dead(self):
        # Close each dead socket.
        while len(self.dead_from_write) > 0:
            old = self.dead_from_write
            self.dead_from_write = []
            for s in old:
                if s.socket is not None:
                    self._close_socket(s)

    def _close_socket(self, s):
        # Close the underlying socket.
        sock = s.socket.fileno()
        s.socket.close()
        # Do not poll on this socket anymore.
        self.poll.unregister(sock)
        # Remove the mapping from this socket FD to its SingleSocket instance.
        del self.single_sockets[sock]
        s.socket = None
        # Notify the Connection object from Encrypter.
        s.handler.connection_lost(s)


# everything below is for testing

class DummyHandler:
    def __init__(self):
        self.external_made = []
        self.data_in = []
        self.lost = []

    def external_connection_made(self, s):
        self.external_made.append(s)
    
    def data_came_in(self, s, data):
        self.data_in.append((s, data))
    
    def connection_lost(self, s):
        self.lost.append(s)

    def connection_flushed(self, s):
        pass

def sl(rs, handler, port):
    rs.bind(port)
    Thread(target = rs.listen_forever, args = [handler]).start()

def loop(rs):
    x = []
    def r(rs = rs, x = x):
        rs.add_task(x[0], .1)
    x.append(r)
    rs.add_task(r, .1)

beginport = 5000 + randrange(10000)

def test_starting_side_close():
    try:
        fa = Event()
        fb = Event()
        da = DummyHandler()
        sa = RawServer(fa, 100, 100)
        loop(sa)
        sl(sa, da, beginport)
        db = DummyHandler()
        sb = RawServer(fb, 100, 100)
        loop(sb)
        sl(sb, db, beginport + 1)

        sleep(.5)
        ca = sa.start_connection(('127.0.0.1', beginport + 1))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert len(db.external_made) == 1
        cb = db.external_made[0]
        del db.external_made[:]
        assert db.data_in == []
        assert db.lost == []

        ca.write('aaa')
        cb.write('bbb')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'bbb')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'aaa')]
        del db.data_in[:]
        assert db.lost == []

        ca.write('ccc')
        cb.write('ddd')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'ddd')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'ccc')]
        del db.data_in[:]
        assert db.lost == []

        ca.close()
        sleep(1)

        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == []
        assert db.lost == [cb]
        del db.lost[:]
    finally:
        fa.set()
        fb.set()

def test_receiving_side_close():
    try:
        da = DummyHandler()
        fa = Event()
        sa = RawServer(fa, 100, 100)
        loop(sa)
        sl(sa, da, beginport + 2)
        db = DummyHandler()
        fb = Event()
        sb = RawServer(fb, 100, 100)
        loop(sb)
        sl(sb, db, beginport + 3)
        
        sleep(.5)
        ca = sa.start_connection(('127.0.0.1', beginport + 3))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert len(db.external_made) == 1
        cb = db.external_made[0]
        del db.external_made[:]
        assert db.data_in == []
        assert db.lost == []

        ca.write('aaa')
        cb.write('bbb')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'bbb')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'aaa')]
        del db.data_in[:]
        assert db.lost == []

        ca.write('ccc')
        cb.write('ddd')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'ddd')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'ccc')]
        del db.data_in[:]
        assert db.lost == []

        cb.close()
        sleep(1)

        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == [ca]
        del da.lost[:]
        assert db.external_made == []
        assert db.data_in == []
        assert db.lost == []
    finally:
        fa.set()
        fb.set()

def test_connection_refused():
    try:
        da = DummyHandler()
        fa = Event()
        sa = RawServer(fa, 100, 100)
        loop(sa)
        sl(sa, da, beginport + 6)

        sleep(.5)
        ca = sa.start_connection(('127.0.0.1', beginport + 15))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == [ca]
        del da.lost[:]
    finally:
        fa.set()

def test_both_close():
    try:
        da = DummyHandler()
        fa = Event()
        sa = RawServer(fa, 100, 100)
        loop(sa)
        sl(sa, da, beginport + 4)

        sleep(1)
        db = DummyHandler()
        fb = Event()
        sb = RawServer(fb, 100, 100)
        loop(sb)
        sl(sb, db, beginport + 5)

        sleep(.5)
        ca = sa.start_connection(('127.0.0.1', beginport + 5))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert len(db.external_made) == 1
        cb = db.external_made[0]
        del db.external_made[:]
        assert db.data_in == []
        assert db.lost == []

        ca.write('aaa')
        cb.write('bbb')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'bbb')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'aaa')]
        del db.data_in[:]
        assert db.lost == []

        ca.write('ccc')
        cb.write('ddd')
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == [(ca, 'ddd')]
        del da.data_in[:]
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == [(cb, 'ccc')]
        del db.data_in[:]
        assert db.lost == []

        ca.close()
        cb.close()
        sleep(1)

        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert db.external_made == []
        assert db.data_in == []
        assert db.lost == []
    finally:
        fa.set()
        fb.set()

def test_normal():
    l = []
    f = Event()
    s = RawServer(f, 100, 100)
    loop(s)
    sl(s, DummyHandler(), beginport + 7)
    s.add_task(lambda l = l: l.append('b'), 2)
    s.add_task(lambda l = l: l.append('a'), 1)
    s.add_task(lambda l = l: l.append('d'), 4)
    sleep(1.5)
    s.add_task(lambda l = l: l.append('c'), 1.5)
    sleep(3)
    assert l == ['a', 'b', 'c', 'd']
    f.set()

def test_catch_exception():
    l = []
    f = Event()
    s = RawServer(f, 100, 100, False)
    loop(s)
    sl(s, DummyHandler(), beginport + 9)
    s.add_task(lambda l = l: l.append('b'), 2)
    s.add_task(lambda: 4/0, 1)
    sleep(3)
    assert l == ['b']
    f.set()

def test_closes_if_not_hit():
    try:
        da = DummyHandler()
        fa = Event()
        sa = RawServer(fa, 2, 2)
        loop(sa)
        sl(sa, da, beginport + 14)

        sleep(1)
        db = DummyHandler()
        fb = Event()
        sb = RawServer(fb, 100, 100)
        loop(sb)
        sl(sb, db, beginport + 13)
        
        sleep(.5)
        sa.start_connection(('127.0.0.1', beginport + 13))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert len(db.external_made) == 1
        del db.external_made[:]
        assert db.data_in == []
        assert db.lost == []

        sleep(3.1)
        
        assert len(da.lost) == 1
        assert len(db.lost) == 1
    finally:
        fa.set()
        fb.set()

def test_does_not_close_if_hit():
    try:
        fa = Event()
        fb = Event()
        da = DummyHandler()
        sa = RawServer(fa, 2, 2)
        loop(sa)
        sl(sa, da, beginport + 12)

        sleep(1)
        db = DummyHandler()
        sb = RawServer(fb, 100, 100)
        loop(sb)
        sl(sb, db, beginport + 13)
        
        sleep(.5)
        sa.start_connection(('127.0.0.1', beginport + 13))
        sleep(1)
        
        assert da.external_made == []
        assert da.data_in == []
        assert da.lost == []
        assert len(db.external_made) == 1
        cb = db.external_made[0]
        del db.external_made[:]
        assert db.data_in == []
        assert db.lost == []

        cb.write('bbb')
        sleep(.5)
        
        assert da.lost == []
        assert db.lost == []
    finally:
        fa.set()
        fb.set()
