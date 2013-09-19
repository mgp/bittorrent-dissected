# Written by Bram Cohen
# see LICENSE.txt for license information

from time import time

class RateMeasure:
    """Measures the time left until a download completes."""

    def __init__(self, left):
        # The time we're measuring the rate since.
        self.start = None
        # The last time data came in.
        self.last = None
        # The measured rate in bytes per second.
        self.rate = 0
        # The estimated seconds until the download completes.
        self.remaining = None
        # The number of bytes left to download.
        self.left = left
        # Whether this download, or corresponding connection, is probably broken.
        self.broke = False
        # True once data has come in.
        self.got_anything = False

    def data_came_in(self, amount):
        if not self.got_anything:
            self.got_anything = True
            self.start = time() - 2
            self.last = self.start
            self.left -= amount
            return
        self.update(time(), amount)

    def data_rejected(self, amount):
        # This data failed a hash check.
        self.left += amount

    def get_time_left(self):
        if not self.got_anything:
            # Cannot estimate time remaining yet.
            return None
        t = time()
        if t - self.last > 15:
            # If we haven't gotten data in awhile, then 
            self.update(t, 0)
        return self.remaining

    def get_size_left(self):
        return self.left

    def update(self, t, amount):
        self.left -= amount
        try:
            # This is the same as Measure.py:
            # self.rate * (self.last - self.start) is (bytes / sec) * sec, and
            # calculates the number of bytes downloaded between self.last and self.start.
            # amount is the bytes downloaded between t (now) and self.last.
            # So the numerator is the total bytes downloaded between t and self.start.
            self.rate = ((self.rate * (self.last - self.start)) + amount) / (t - self.start)
            self.last = t
            # (bytes) / (bytes / sec) leaves the seconds remaining.
            self.remaining = self.left / self.rate
            if self.start < self.last - self.remaining:
                # Keeps self.last - self.start from dominating the computation.
                self.start = self.last - self.remaining
        except ZeroDivisionError:
            # If self.rate is 0, then the following conditionals could execute as well.
            self.remaining = None
        if self.broke and self.last - self.start < 20:
            self.start = self.last - 20
        if self.last - self.start > 20:
            self.broke = True
