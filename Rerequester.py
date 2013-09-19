# Written by Bram Cohen
# see LICENSE.txt for license information

from zurllib import urlopen, quote
from btformats import check_peers
from bencode import bdecode
from threading import Thread, Lock
from socket import error, gethostbyname
from time import time
from random import randrange
from binascii import b2a_hex

class Rerequester:
    def __init__(self, url, interval, sched, howmany, minpeers, 
            connect, externalsched, amount_left, up, down,
            port, ip, myid, infohash, timeout, errorfunc, maxpeers, doneflag,
            upratefunc, downratefunc, ever_got_incoming):
        # The URL and query paramters to always pass.
        self.url = ('%s?info_hash=%s&peer_id=%s&port=%s&key=%s' %
            (url, quote(infohash), quote(myid), str(port),
            b2a_hex(''.join([chr(randrange(256)) for i in xrange(4)]))))
        # The IP address of this client.
        self.ip = ip
        # The time in seconds between requesting more peers.
        self.interval = interval
        # The last time this client got a reply from the tracker.
        self.last = None
        # The identifier returned by the tracker, which this client uses on subsequent requests.
        self.trackerid = None
        # Maximum seconds between sending requests to the tracker.
        self.announce_interval = 30 * 60
        # Function to schedule events in the reactor loop of RawServer.
        self.sched = sched
        # Method that returns how many peers this client is connected to.
        self.howmany = howmany
        # If connected to this many peers, may skip making a request to the tracker.
        self.minpeers = minpeers
        # Method on Connecter that starts a connection to a peer.
        self.connect = connect
        # Function to schedule events in the reactor loop of RawServer.
        self.externalsched = externalsched
        # Method to get the amount of data left.
        self.amount_left = amount_left
        # Method to get the total bytes uploaded.
        self.up = up
        # Method to get the total bytes downloaded.
        self.down = down
        # HTTP timeout when making a request to the tracker.
        self.timeout = timeout
        # Callback invoked with a string describing any error.
        self.errorfunc = errorfunc
        # If connected to this many peers, will not request any more from the tracker.
        self.maxpeers = maxpeers
        # Flag set if we have all pieces and are seeding.
        self.doneflag = doneflag
        # Method to get the upload rate.
        self.upratefunc = upratefunc
        # Method to get the download rate.
        self.downratefunc = downratefunc
        # Method that returns True if we ever got an incoming connection.
        self.ever_got_incoming = ever_got_incoming
        # True if the last request to the tracker failed.
        self.last_failed = True
        # The last time this client made a request to the tracker.
        self.last_time = 0

    def c(self):
        # Call this method again later.
        self.sched(self.c, self.interval)
        # Determine if we need more peers from the tracker.
        if self.ever_got_incoming():
            # Got an incoming connection.
            getmore = self.howmany() <= self.minpeers / 3
        else:
            # Never got an incoming connection.
            # Assume this client is behind a NAT, and aggressively try and connect to other peers.
            getmore = self.howmany() < self.minpeers
        if getmore or time() - self.last_time > self.announce_interval:
            # Need to connect to more peers, or need to simply check-in with the tracker.
            self.announce()

    def begin(self):
        # Method c is the method called at regular intervals to contact the tracker.
        self.sched(self.c, self.interval)
        # But contact the tracker now. Setting event = 0 specifies starting the download.
        self.announce(0)

    def announce(self, event = None):
        # Update the time we last made a request to the tracker.
        self.last_time = time()
        # Append total uploaded, total downloaded, and bytes left to download. 
        s = ('%s&uploaded=%s&downloaded=%s&left=%s' %
            (self.url, str(self.up()), str(self.down()), 
            str(self.amount_left())))
        if self.last is not None:
            # Append the last time this client made a request to the tracker.
            s += '&last=' + quote(str(self.last))
        if self.trackerid is not None:
            # If not the first request, append the id this tracker previously returned.
            s += '&trackerid=' + quote(str(self.trackerid))
        if self.howmany() >= self.maxpeers:
            # Don't need any more peers to connect to.
            s += '&numwant=0'
        else:
            # Return peer IP and port addresses in 6 binary bytes.
            s += '&compact=1'
        # Event is not specified if this request is one performed at regular intervals.
        if event != None:
            s += '&event=' + ['started', 'completed', 'stopped'][event]
        # Method that returns True the first time and False every subsequent time.
        set = SetOnce().set
        def checkfail(self = self, set = set):
            if set():
                # Only get here if the tracker did not reply and call set() in rerequest first.
                if self.last_failed and self.upratefunc() < 100 and self.downratefunc() < 100:
                    self.errorfunc('Problem connecting to tracker - timeout exceeded')
                self.last_failed = True
        # Method checkfail will run if the tracker does not reply to this request.
        self.sched(checkfail, self.timeout)
        Thread(target = self.rerequest, args = [s, set]).start()

    def rerequest(self, url, set):
        # url is s from method announce.
        try:
            if self.ip:
                # Include our IP address in case we are communicating through a proxy.
                url += '&ip=' + gethostbyname(self.ip)
            # Read a reply.
            h = urlopen(url)
            r = h.read()
            h.close()
            if set():
                # Only get here if checkfail did not run and call set() first.
                def add(self = self, r = r):
                    # This call succeeded.
                    self.last_failed = False
                    # Process the reply.
                    self.postrequest(r)
                self.externalsched(add, 0)
        except (IOError, error), e:
            if set():
                # Only get here if checkfail did not run and call set() first.
                def fail(self = self, r = 'Problem connecting to tracker - ' + str(e)):
                    if self.last_failed:
                        self.errorfunc(r)
                    self.last_failed = True
                self.externalsched(fail, 0)

    def postrequest(self, data):
        try:
            r = bdecode(data)
            check_peers(r)
            if r.has_key('failure reason'):
                self.errorfunc('rejected by tracker - ' + r['failure reason'])
            else:
                if r.has_key('warning message'):
                    self.errorfunc('warning from tracker - ' + r['warning message'])
                self.announce_interval = r.get('interval', self.announce_interval)
                self.interval = r.get('min interval', self.interval)
                self.trackerid = r.get('tracker id', self.trackerid)
                self.last = r.get('last')
                p = r['peers']
                peers = []
                if type(p) == type(''):
                    # Deserialize the compact binary form.
                    for x in xrange(0, len(p), 6):
                        ip = '.'.join([str(ord(i)) for i in p[x:x+4]])
                        port = (ord(p[x+4]) << 8) | ord(p[x+5])
                        peers.append((ip, port, None))
                else:
                    for x in p:
                        peers.append((x['ip'], x['port'], x.get('peer id')))

                ps = len(peers) + self.howmany()
                if ps < self.maxpeers:
                    if self.doneflag.isSet():
                        if r.get('num peers', 1000) - r.get('done peers', 0) > ps * 1.2:
                            self.last = None
                    else:
                        if r.get('num peers', 1000) > ps * 1.2:
                            self.last = None
                for x in peers:
                    self.connect((x[0], x[1]), x[2])
        except ValueError, e:
            if data != '':
                self.errorfunc('bad data from tracker - ' + str(e))

class SetOnce:
    def __init__(self):
        self.lock = Lock()
        self.first = True

    def set(self):
        """Returns True on the first call, and False on every subsequent call."""
        try:
            self.lock.acquire()
            r = self.first
            self.first = False
            return r
        finally:
            self.lock.release()

