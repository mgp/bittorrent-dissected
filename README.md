bittorrent-dissected
====================

The BitTorrent mainline client commented and explained. This excludes the GUI, and focuses only on the downloader (`download.py` and its dependencies), not the tracker (`track.py`). The code is copied from [its CVS repository on SourceForge](http://bittorrent.cvs.sourceforge.net/viewvc/bittorrent/BitTorrent/BitTorrent/).

It's probably best to [read the BitTorrent protocol specification](https://wiki.theory.org/BitTorrentSpecification) before reading the implementation.

## Classes

Below are all the classes comprising the downloader.

### Networking

The networking layer that sends and receives data with peers.

#### `selectpoll.py`

A drop in replacement for `poll` if the version of Python does not support it. It uses `select` instead, which is less efficient.

#### `RawServer.py`

A reactor loop that relies on either `poll` or `selectpoll`. It specifies:

* scheduling tasks within the loop
* connecting to a given address, which invokes methods on a given handler depending on whether it succeeds or fails
* listening for new connections on a given port, which invokes a method on a handler the server is constructed with
* closing/removing connections that have timed out or cannot be written to, which also notifies the handler
* running the loop until a flag is asynchronously set

It defines a helper class named `SingleSocket` that wraps the socket. It specifies:

* whether the socket is still connecting or has connected
* a handler that is invoked whenever data is received, or all queued data has been written
* bytes enqueued for sending
* the last time data was read from the socket so it can be monitored for timeouts

### Storage

The storage layer that reads and writes bytes on disk.

#### `Storage.py`

The low-level byte offset-oriented storage interface.

* takes a sequence of (file, length) pairs
* can read data spanning multiple files given a first byte offset and length
* can write data spanning multiple files given that data and a first byte offset

#### `StorageWrapper.py`

The high-level piece and block storage interface.

* takes the SHA-1 hashes, piece size, and block size
* maintains the bitfield of what pieces we have
* manages what byte offsets pieces are written to on disk; pieces may be written to earlier positions to minimize "holes" and limit space usage
* after the piecepicker decides to begin downloading a piece, tracks what blocks are still missing
* once all block for a piece are written, validates the SHA-1 hash of the piece
* once a piece has been validated, sets the corresponding bit in the bitfield
* tracks when in endgame mode, combining the bitfield with what blocks are missing for downloading pieces
* tracks when file is finished

### Miscellaneous

Standalone classes that the downloader classes below use.

#### `parseargs.py`

Parses command line arguments.

#### `fakeopen.py`

Classes used for testing with dependency injection:

* class `FakeHandle` is a fake file handle with `seek`, `read`, `write`, `flush`, and `close` methods
* class `FakeOpen` is a fake filesystem with `open`, `exists`, and `getsize` methods

#### `btformats.py`

Methods to validate data that has been bdecoded.

* method `check_peers` validates a response from the tracker
* method `check_info` validates the metainfo data, which is contained in the `.torrent` file

#### `bitfield.py`

An array of boolean values. Used to represent what pieces each peer has, as well as what pieces the client has.

* tracks whether all values are `True`; when used to track downloaded pieces, this means the file is complete
* can convert to an actual bit array for sending over the wire

#### `bencode.py`

Methods to convert between bencoded form and Python objects.

* method `bdecode` converts from a bencoded string to Python objects
* method `bencode` converts from Python objects to a bencoded string

#### `CurrentRateMeasure.py`

Computes the rate in bytes/second of a data flow.

* averages rate over last `max_rate_period` seconds, so rates from long ago have little impact
* if measuring upload rate, can compute the time until the rate falls below a given value after temporarily suspending uploads

#### `RateMeasure.py`

Computes the time until a download completes.

* like `CurrentRateMeasure.py`, attempts to minimize rates from long ago dominating the computation 
* can also return the bytes remaining, since required to compute the time remaining

#### `Choker.py`

Decides which peers to choke.

* every 10 seconds unchokes the `max_upload - 1` fastest peers who are interested
* every 30 seconds optimistically unchokes one of the peers who is also interested but not the fastest
* if client is a peer, we unchoke peers who are uploading to us the fastest to try for pareto efficiency
* if client is a seed, we unchoke peers who are downloading the fastest to disseminate data as fast as possible
* may also change who we choke upon peers connecting or disconnecting, or becoming interested or uninterested

#### `PiecePicker.py`

Decides what piece to download from a peer. `StorageWrapper.py` decides what block to download.

* adds together the bitfields for pieces each peer has
* knows what pieces have been downloaded, what pieces are downloading, and the availability of the remaining pieces
* picks the rarest piece that is already downloading from another peer or seed in order to finish it faster
* if picking one of the first pieces to download, pick a random piece to download from the peer
* if peer does not have a piece that's already being downloaded, and not picking one of the first pieces, pick one of the rarest

#### `Rerequester.py`

Communicates with the tracker, primarily to find new peers.

* checks in while requesting more peers every `interval` seconds if it has less than `minpeers`
* always checks in every `announce_interval` seconds, and requests more peers if it has less than `maxpeers`
* also check in whenever the download is started, stopped, or completed
* attempts to connect to new peers immediately

### Downloader

The classes that combine the networking, storage, and standalone classes above. These classes contain the majority of the high-level logic from the BitTorrent protocol specification.

#### `Download.py`

Manages the state for downloading from a peer.

* maintains whether this client is interested in the peer, and whether this peer is choking the client
* maintains the bitfield of pieces this peer has, what blocks the client has requested, and a `Measure` instance for download
* when a block is downloaded, writes it to `StorageWrapper`, and updates the `PiecePicker` if it completes a piece
* when a block is downloaded or the peer sends a have message, makes a new request for a block if possible
* also monitors entering endgame mode, where a `Download` instance requests blocks belonging to every other `Download` instance

#### `Upload.py`

Manages the state for uploading to a peer.

* maintains whether this peer is interested in this client, and whether this client is choking the peer
* has a queue of blocks requests by this peer, and a `Measure` instance for the upload rate
* reads requested blocks from `StorageWrapper` and writes them to the connection
* clears the queue of blocks whenever the peer becomes uninterested, or we choke the peer

#### `Connecter.py`

Manages established connections.

* suspends upload rate if it exceeds the maximum, and then resumes it after a calculated time
* decodes each incoming message
* choke, unchoke, have, bitfield, and piece messages go to the `Download` object of a `Connection`
* interested, uninterested, piece request, and their cancel messages go to the `Upload` object of a `Connection`

It defines a helper class named `Connection` that wraps the `Connection` from `Encrypter`. It specifies:

* a `Download` instance to hold download state, and an `Upload` instance to hold upload state
* methods that write every message type to its wrapped `Connection`, which delegates to the `SingleSocket` instance

#### `Encrypter.py`

This module doesn't actually define a class called `Encrypter`. It defines an `Encoder` class, which `RawServer` uses as its handler:

* it schedules sending keep alive messages to each peer
* it tracks whether any incoming connection was established, to tell if behind a firewall
* notified by `RawServer` when connections are created, destroyed, flushed, or when data comes in
* only notifies the `Connecter` instance of fully established connections to peers

It defines a helper class named `Connection` that wraps the `SingleSocket` from `RawServer`. It specifies:

* a buffer for accumulated data from the peer
* a state machine to read the header, download id, peer id, and then endless messages from the peer
* delegates to the `Connecter` whenever an incoming connection is made or a message is read

#### `download.py`

The top-level module, or the main program. It does, in order:

* parse the command line arguments
* read and bdecode the metainfo (torrent) file
* make the directory structure of files
* create the networking layer, or `RawServer`
* create the storage layer, or `Storage` wrapped in `StorageWrapper`
* create the `Choker`, `RateMeasure`, `PiecePicker`, and upload and download `Measure` instances
* create the `Downloader`, which owns the `StorageWrapper`, `PiecePicker`, and download `Measure` instance
* create the `Connecter`, which owns the `Upload` factory, `Downloader`, `Choker`, and upload `Measure` instance
* create the `Encoder`, which owns the `Connecter` and `RawServer` instance
* create the `Rerequester` and begin connecting to the tracker
* listen forever on the `RawServer`, and pass the `Encoder` as its event handler

