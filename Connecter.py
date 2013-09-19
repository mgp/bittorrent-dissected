# Written by Bram Cohen
# see LICENSE.txt for license information

from bitfield import Bitfield
from binascii import b2a_hex
from CurrentRateMeasure import Measure

def toint(s):
    return long(b2a_hex(s), 16)

def tobinary(i):
    return (chr(i >> 24) + chr((i >> 16) & 0xFF) + 
        chr((i >> 8) & 0xFF) + chr(i & 0xFF))

CHOKE = chr(0)
UNCHOKE = chr(1)
INTERESTED = chr(2)
NOT_INTERESTED = chr(3)
# index
HAVE = chr(4)
# index, bitfield
BITFIELD = chr(5)
# index, begin, length
REQUEST = chr(6)
# index, begin, piece
PIECE = chr(7)
# index, begin, piece
CANCEL = chr(8)

class Connection:
    def __init__(self, connection, connecter):
        # The Connection instance from the Encrypter module.
        self.connection = connection
        # The Connecter instance.
        self.connecter = connecter
        # True if this client ever got a message from this peer.
        self.got_anything = False

    def get_ip(self):
        return self.connection.get_ip()

    def get_id(self):
        return self.connection.get_id()

    def close(self):
        self.connection.close()

    def is_flushed(self):
        if self.connecter.rate_capped:
            return False
        return self.connection.is_flushed()

    def is_locally_initiated(self):
        return self.connection.is_locally_initiated()

    def send_interested(self):
        self.connection.send_message(INTERESTED)

    def send_not_interested(self):
        self.connection.send_message(NOT_INTERESTED)

    def send_choke(self):
        self.connection.send_message(CHOKE)

    def send_unchoke(self):
        self.connection.send_message(UNCHOKE)

    def send_request(self, index, begin, length):
        # Send a request to this peer for a block in a given piece.
        self.connection.send_message(REQUEST + tobinary(index) + 
            tobinary(begin) + tobinary(length))

    def send_cancel(self, index, begin, length):
        # Cancel a request to this peer for a block in a given piece.
        self.connection.send_message(CANCEL + tobinary(index) + 
            tobinary(begin) + tobinary(length))

    def send_piece(self, index, begin, piece):
        assert not self.connecter.rate_capped
        # Update the aggregate upload rate to all peers.
        self.connecter._update_upload_rate(len(piece))
        # Send a block requested by this peer.
        self.connection.send_message(PIECE + tobinary(index) + 
            tobinary(begin) + piece)

    def send_bitfield(self, bitfield):
        # Send to this peer the bitfield of pieces this client has.
        self.connection.send_message(BITFIELD + bitfield)

    def send_have(self, index):
        # Send to this peer a piece that this client now has.
        self.connection.send_message(HAVE + tobinary(index))

    def get_upload(self):
        # Return the upload object assigned to this instance by Connecter.
        return self.upload

    def get_download(self):
        # Return the download object assigned to this instance by Connecter.
        return self.download

class Connecter:
    def __init__(self, make_upload, downloader, choker, numpieces,
            totalup, max_upload_rate = 0, sched = None):
        # This is a Downloader that returns SingleDownload instances from Downloader.py.
        self.downloader = downloader
        # This creates instances of Upload from Uploader.py.
        self.make_upload = make_upload
        # The Choker instance from Choker.py.
        self.choker = choker
        # The number of pieces from the metainfo file.
        self.numpieces = numpieces
        # The upload rate to cap at.
        self.max_upload_rate = max_upload_rate
        # Function to schedule events in the reactor loop of RawServer.
        self.sched = sched
        # The Measure instance that measures our aggregate upload rate to all peers.
        self.totalup = totalup
        # Whether this client is temporarily suspending uploads.
        self.rate_capped = False
        # Maps each Connection from Encrypter to its Connection instance defined above.
        self.connections = {}

    def _update_upload_rate(self, amount):
        # Update the aggregate upload rate to all peers.
        self.totalup.update_rate(amount)
        if self.max_upload_rate > 0 and self.totalup.get_rate_noupdate() > self.max_upload_rate:
            # We have exceeded the maximum upload rate, so suspend them for now.
            self.rate_capped = True
            # Schedule resuming uploads after suspending drops us below the maximum rate again.
            self.sched(self._uncap, self.totalup.time_until_rate(self.max_upload_rate))

    def _uncap(self):
        self.rate_capped = False
        while not self.rate_capped:
            # The Upload instance with the slowest upload rate.
            up = None
            minrate = None
            # Find the Upload object 
            for i in self.connections.values():
                if not i.upload.is_choked() and i.upload.has_queries() and i.connection.is_flushed():
                    # This peer is not choked by the client, has outstanding block requests,
                    # and did not encounter backpressure and so sending data might succeed.
                    rate = i.upload.get_rate()
                    if up is None or rate < minrate:
                        # This is the peer with the slowest upload rate.
                        up = i.upload
                        minrate = rate
            if up is None:
                break
            # Upload to this peer.
            # This can, in turn, call _update_upload_rate and set rate_capped again.
            up.flushed()
            if self.totalup.get_rate_noupdate() > self.max_upload_rate:
                break

    def change_max_upload_rate(self, newval):
        def foo(self=self, newval=newval):
            self._change_max_upload_rate(newval)
        self.sched(foo, 0);
        
    def _change_max_upload_rate(self, newval):
        self.max_upload_rate = newval
        self._uncap()
        
    def how_many_connections(self):
        return len(self.connections)

    def connection_made(self, connection):
        # Map from the Connection from the Encrypter module to the Connection defined above.
        c = Connection(connection, self)
        self.connections[connection] = c
        c.upload = self.make_upload(c)
        c.download = self.downloader.make_download(c)
        # Notify the choker that this peer connected, to maybe optimistically unchoke it.
        self.choker.connection_made(c)

    def connection_lost(self, connection):
        c = self.connections[connection]
        d = c.download
        # Remove mapping to the Connection instance.
        del self.connections[connection]
        # Notify the downloader that disconnected.
        d.disconnected()
        # Notify the choker that this peer disconnected, to maybe unchoke a different peer.
        self.choker.connection_lost(c)

    def connection_flushed(self, connection):
        self.connections[connection].upload.flushed()

    def got_message(self, connection, message):
        c = self.connections[connection]
        t = message[0]
        if t == BITFIELD and c.got_anything:
            # If we are not receiving the bitfield first, close this connection.
            connection.close()
            return
        # Got at least one message from this peer.
        c.got_anything = True
        if (t in [CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED] and 
                len(message) != 1):
            connection.close()
            return
        if t == CHOKE:
            # Choke messages affect downloading.
            c.download.got_choke()
        elif t == UNCHOKE:
            # Unchoke messages affect downloading.
            c.download.got_unchoke()
        elif t == INTERESTED:
            # Interested messages affect uploading.
            c.upload.got_interested()
        elif t == NOT_INTERESTED:
            # Uninterested messages affect uploading.
            c.upload.got_not_interested()
        elif t == HAVE:
            # A peer having a new piece affects downloading.
            if len(message) != 5:
                connection.close()
                return
            i = toint(message[1:])
            if i >= self.numpieces:
                connection.close()
                return
            c.download.got_have(i)
        elif t == BITFIELD:
            # A peer sending all pieces it has affects downloading.
            try:
                b = Bitfield(self.numpieces, message[1:])
            except ValueError:
                connection.close()
                return
            c.download.got_have_bitfield(b)
        elif t == REQUEST:
            # A peer requesting a block affects uploading.
            if len(message) != 13:
                connection.close()
                return
            i = toint(message[1:5])
            if i >= self.numpieces:
                connection.close()
                return
            c.upload.got_request(i, toint(message[5:9]), 
                toint(message[9:]))
        elif t == CANCEL:
            # A peer cancelling a request for a block affects uploading.
            if len(message) != 13:
                connection.close()
                return
            i = toint(message[1:5])
            if i >= self.numpieces:
                connection.close()
                return
            c.upload.got_cancel(i, toint(message[5:9]), 
                toint(message[9:]))
        elif t == PIECE:
            # A requested block sent to this client by a peer affects downloading.
            if len(message) <= 9:
                connection.close()
                return
            i = toint(message[1:5])
            if i >= self.numpieces:
                connection.close()
                return
            if c.download.got_piece(i, toint(message[5:9]), message[9:]):
                for co in self.connections.values():
                    co.send_have(i)
        else:
            # This is an unknown message type, so close this connection.
            connection.close()


class DummyUpload:
    def __init__(self, events):
        self.events = events
        events.append('made upload')

    def flushed(self):
        self.events.append('flushed')

    def got_interested(self):
        self.events.append('interested')
        
    def got_not_interested(self):
        self.events.append('not interested')

    def got_request(self, index, begin, length):
        self.events.append(('request', index, begin, length))

    def got_cancel(self, index, begin, length):
        self.events.append(('cancel', index, begin, length))

class DummyDownload:
    def __init__(self, events):
        self.events = events
        events.append('made download')
        self.hit = 0

    def disconnected(self):
        self.events.append('disconnected')

    def got_choke(self):
        self.events.append('choke')

    def got_unchoke(self):
        self.events.append('unchoke')

    def got_have(self, i):
        self.events.append(('have', i))

    def got_have_bitfield(self, bitfield):
        self.events.append(('bitfield', bitfield.tostring()))

    def got_piece(self, index, begin, piece):
        self.events.append(('piece', index, begin, piece))
        self.hit += 1
        return self.hit > 1

class DummyDownloader:
    def __init__(self, events):
        self.events = events

    def make_download(self, connection):
        return DummyDownload(self.events)

class DummyConnection:
    def __init__(self, events):
        self.events = events

    def send_message(self, message):
        self.events.append(('m', message))

class DummyChoker:
    def __init__(self, events, cs):
        self.events = events
        self.cs = cs

    def connection_made(self, c):
        self.events.append('made')
        self.cs.append(c)

    def connection_lost(self, c):
        self.events.append('lost')

def test_operation():
    events = []
    cs = []
    co = Connecter(lambda c, events = events: DummyUpload(events), 
        DummyDownloader(events), DummyChoker(events, cs), 3, 
        Measure(10))
    assert events == []
    assert cs == []
    
    dc = DummyConnection(events)
    co.connection_made(dc)
    assert len(cs) == 1
    cc = cs[0]
    co.got_message(dc, BITFIELD + chr(0xc0))
    co.got_message(dc, CHOKE)
    co.got_message(dc, UNCHOKE)
    co.got_message(dc, INTERESTED)
    co.got_message(dc, NOT_INTERESTED)
    co.got_message(dc, HAVE + tobinary(2))
    co.got_message(dc, REQUEST + tobinary(1) + tobinary(5) + tobinary(6))
    co.got_message(dc, CANCEL + tobinary(2) + tobinary(3) + tobinary(4))
    co.got_message(dc, PIECE + tobinary(1) + tobinary(0) + 'abc')
    co.got_message(dc, PIECE + tobinary(1) + tobinary(3) + 'def')
    co.connection_flushed(dc)
    cc.send_bitfield(chr(0x60))
    cc.send_interested()
    cc.send_not_interested()
    cc.send_choke()
    cc.send_unchoke()
    cc.send_have(4)
    cc.send_request(0, 2, 1)
    cc.send_cancel(1, 2, 3)
    cc.send_piece(1, 2, 'abc')
    co.connection_lost(dc)
    x = ['made upload', 'made download', 'made', 
        ('bitfield', chr(0xC0)), 'choke', 'unchoke',
        'interested', 'not interested', ('have', 2), 
        ('request', 1, 5, 6), ('cancel', 2, 3, 4),
        ('piece', 1, 0, 'abc'), ('piece', 1, 3, 'def'), 
        ('m', HAVE + tobinary(1)),
        'flushed', ('m', BITFIELD + chr(0x60)), ('m', INTERESTED), 
        ('m', NOT_INTERESTED), ('m', CHOKE), ('m', UNCHOKE), 
        ('m', HAVE + tobinary(4)), ('m', REQUEST + tobinary(0) + 
        tobinary(2) + tobinary(1)), ('m', CANCEL + tobinary(1) + 
        tobinary(2) + tobinary(3)), ('m', PIECE + tobinary(1) + 
        tobinary(2) + 'abc'), 'disconnected', 'lost']
    for a, b in zip (events, x):
        assert a == b, repr((a, b))

def test_conversion():
    assert toint(tobinary(50000)) == 50000
