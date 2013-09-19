# Written by Bram Cohen
# see LICENSE.txt for license information

from sha import sha
from bisect import bisect_right

class Storage:
    def __init__(self, files, open, exists, getsize):
        # can raise IOError and ValueError
        # Contains an array of (first byte offset, end byte offset, filename) tuples.
        self.ranges = []
        total = 0l
        so_far = 0l
        for file, length in files:
            if length != 0:
                self.ranges.append((total, total + length, file))
                total += length
                if exists(file):
                    l = getsize(file)
                    if l > length:
                        l = length
                    so_far += l
            elif not exists(file):
                open(file, 'wb').close()
        # The first byte offset for each filename.
        self.begins = [i[0] for i in self.ranges]
        # The total bytes to download and save.
        self.total_length = total
        # Maps each filename to its file handle.
        self.handles = {}
        # Maps a filename to 1 if it is opened for writing.
        self.whandles = {}
        # Maps a filename to its existing size upon startup.
        self.tops = {}
        for file, length in files:
            if exists(file):
                # Will append to the existing file.
                l = getsize(file)
                if l != length:
                    # Not the correct size; open in append and binary mode for writing.
                    self.handles[file] = open(file, 'rb+')
                    self.whandles[file] = 1
                    if l > length:
                        self.handles[file].truncate(length)
                else:
                    # Already the correct size; open in read and binary mode.
                    self.handles[file] = open(file, 'rb')
                # Although it may have been truncated, the file existed with this size.
                self.tops[file] = l
            else:
                # File does not exist; open in append and binary mode while clobbering.
                self.handles[file] = open(file, 'wb+')
                self.whandles[file] = 1

    def was_preallocated(self, pos, length):
        # Preallocated if each overlapping file was at least its full size on startup.
        for file, begin, end in self._intervals(pos, length):
            if self.tops.get(file, 0) < end:
                return False
        return True

    def set_readonly(self):
        # may raise IOError or OSError
        for file in self.whandles.keys():
            old = self.handles[file]
            old.flush()
            old.close()
            # Open in read and binary mode.
            self.handles[file] = open(file, 'rb')

    def get_total_length(self):
        return self.total_length

    def _intervals(self, pos, amount):
        # An array of overlapping (filename, first_byte, end_byte) tuples.
        # Concatenated these file ranges are equal to the byte range [pos, pos + amount).
        r = []
        # The end byte offset.
        stop = pos + amount
        p = bisect_right(self.begins, pos) - 1
        # self.ranges[p] < stop is also begins[p] < stop
        while p < len(self.ranges) and self.ranges[p][0] < stop:
            begin, end, file = self.ranges[p]
            r.append((file, max(pos, begin) - begin, min(end, stop) - begin))
            p += 1
        return r

    def read(self, pos, amount):
        # Concatenate the contents of the returned file ranges.
        r = []
        for file, pos, end in self._intervals(pos, amount):
            h = self.handles[file]
            h.seek(pos)
            r.append(h.read(end - pos))
        return ''.join(r)

    def write(self, pos, s):
        # might raise an IOError
        total = 0
        for file, begin, end in self._intervals(pos, len(s)):
            if not self.whandles.has_key(file):
                # This file was only open for reading; open in append and binary mode.
                self.handles[file].close()
                self.handles[file] = open(file, 'rb+')
                self.whandles[file] = 1
            h = self.handles[file]
            # May skip some bytes on the first file. For subsequent files begin = 0.
            h.seek(begin)
            h.write(s[total: total + end - begin])
            total += end - begin

    def close(self):
        for h in self.handles.values():
            h.close()


def lrange(a, b, c):
    r = []
    while a < b:
        r.append(a)
        a += c
    return r


# everything below is for testing

from fakeopen import FakeOpen

def test_Storage_simple():
    f = FakeOpen()
    m = Storage([('a', 5)], f.open, f.exists, f.getsize)
    assert f.files.keys() == ['a']
    m.write(0, 'abc')
    assert m.read(0, 3) == 'abc'
    m.write(2, 'abc')
    assert m.read(2, 3) == 'abc'
    m.write(1, 'abc')
    assert m.read(0, 5) == 'aabcc'
    
def test_Storage_multiple():
    f = FakeOpen()
    m = Storage([('a', 5), ('2', 4), ('c', 3)], 
        f.open, f.exists, f.getsize)
    x = f.files.keys()
    x.sort()
    assert x == ['2', 'a', 'c']
    m.write(3, 'abc')
    assert m.read(3, 3) == 'abc'
    m.write(5, 'ab')
    assert m.read(4, 3) == 'bab'
    m.write(3, 'pqrstuvw')
    assert m.read(3, 8) == 'pqrstuvw'
    m.write(3, 'abcdef')
    assert m.read(3, 7) == 'abcdefv'

def test_Storage_zero():
    f = FakeOpen()
    Storage([('a', 0)], f.open, f.exists, f.getsize)
    assert f.files == {'a': []}

def test_resume_zero():
    f = FakeOpen({'a': ''})
    Storage([('a', 0)], f.open, f.exists, f.getsize)
    assert f.files == {'a': []}

def test_Storage_with_zero():
    f = FakeOpen()
    m = Storage([('a', 3), ('b', 0), ('c', 3)], 
        f.open, f.exists, f.getsize)
    m.write(2, 'abc')
    assert m.read(2, 3) == 'abc'
    x = f.files.keys()
    x.sort()
    assert x == ['a', 'b', 'c']
    assert len(f.files['a']) == 3
    assert len(f.files['b']) == 0

def test_Storage_resume():
    f = FakeOpen({'a': 'abc'})
    m = Storage([('a', 4)], 
        f.open, f.exists, f.getsize)
    assert f.files.keys() == ['a']
    assert m.read(0, 3) == 'abc'

def test_Storage_mixed_resume():
    f = FakeOpen({'b': 'abc'})
    m = Storage([('a', 3), ('b', 4)], 
        f.open, f.exists, f.getsize)
    x = f.files.keys()
    x.sort()
    assert x == ['a', 'b']
    assert m.read(3, 3) == 'abc'
