class Follower:
    def __init__(self, next_index):
        self.next = next_index
        self.last = 0

    def behind_snapshot(self, snapshot):
        return self.next <= snapshot.last_index()

    def set_last_index(self, index):
        if self.last >= index:
            return  # last_index is monotonically increasing

        self.last = index

    def set_next_index(self, index):
        self.next = index

    def next_index(self):
        return self.next

    def last_index(self):
        return self.last