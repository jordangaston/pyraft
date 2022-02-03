class ElectionTimer:
    def __init__(self, timeout_range, prng):
        self.timeout_range = timeout_range
        self.prng = prng

    def next_timeout(self):
        return self.prng.randint(self.timeout_range[0], self.timeout_range[1])