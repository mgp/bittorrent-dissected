# Written by Bram Cohen
# see LICENSE.txt for license information

from time import time

class Measure:
    """Measures upload or download rate."""

    def __init__(self, max_rate_period, fudge = 1):
        self.max_rate_period = max_rate_period
        # The time we're measuring the rate since.
        self.ratesince = time() - fudge
        # The last time this instance updated its rate measurement.
        self.last = self.ratesince
        # The measured rate in bytes per second.
        self.rate = 0.0
        # The total bytes received.
        self.total = 0l

    def update_rate(self, amount):
        self.total += amount
        t = time()
        # self.rate * (self.last - self.ratesince) is (bytes / sec) * sec, and
        # calculates the number of bytes downloaded between self.last and self.ratesince.
        # amount is the bytes downloaded between t (now) and self.last.
        # So the numerator is the total bytes downloaded between t and self.ratesince.
        self.rate = (self.rate * (self.last - self.ratesince) + 
            amount) / (t - self.ratesince)
        self.last = t
        if self.ratesince < t - self.max_rate_period:
            # Keeps self.last - self.ratesince from dominating the computation.
            self.ratesince = t - self.max_rate_period

    def get_rate(self):
        self.update_rate(0)
        return self.rate

    def get_rate_noupdate(self):
        return self.rate

    def time_until_rate(self, newrate):
        # After suspending uploads, this returns how long until we drop below the given rate.
        if self.rate <= newrate:
            # Below the given rate, so can resume uploads immediately.
            return 0
        t = time() - self.ratesince
        # This returns time_until, where newrate * (time_until + t) = self.rate * t.
        # This is equivalent to newrate * (time_until + t) = (self.rate * t) + (0 * time_until),
        # where 0 is the rate over the next time_until seconds.
        return ((self.rate * t) / newrate) - t

    def get_total(self):
        return self.total
