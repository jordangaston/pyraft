from typing import MutableMapping
from ds_from_scratch.raft.model.log import LogEntry


class PickleDbLogStore:

    def __init__(self, db):
        self.db = db
        self.len = 0
        self.__init_log()

    def __setitem__(self, index, log_entry):
        if type(index) is not int:
            raise ValueError('type of index must be int')

        if index < 0 or index >= self.len:
            raise IndexError('log index out of range')

        self.db.dadd('log', (str(index), log_entry.__dict__))
        self.db.dump()

    def __getitem__(self, index):
        if type(index) is int:
            return self.__get_item(index)
        elif type(index) is slice:
            return self.__get_range(index)
        else:
            raise ValueError('type of index must be int or slice')

    def __iter__(self):
        return self.Iterator(self)

    def __len__(self):
        return self.len

    def pop(self, index=None):
        if index is None:
            index = self.len - 1

        if self.len == 0:
            raise IndexError('pop from empty list')

        if index >= self.len or index < 0:
            raise IndexError('pop index out of range')

        if self.len == 1:
            popped = self[0]
            self.db.dpop('log', str(0))
            self.len -= 1
            self.db.dump()
            return popped

        if index == self.len - 1:
            popped = self[self.len - 1]
            self.db.dpop('log', str(self.len - 1))
            self.len -= 1
            self.db.dump()
            return popped

        popped = self[index]

        length = self.len - 1
        for i in range(index, length):
            self[i] = self[i + 1]
            self.db.dpop('log', str(i + 1))
            self.len -= 1

        self.db.dump()

        return popped

    def clear(self):
        self.db.drem('log')
        self.len = 0
        self.db.dcreate('log')
        self.db.dump()

    def copy(self):
        values = []
        for value in self.db.dgetall('log').values():
            values.append(self.__to_entry(value))
        return values

    def append(self, *entries):
        for entry in entries:
            self.db.dadd('log', (str(self.len), entry.__dict__))
            self.len += 1
        self.db.dump()

    def __get_range(self, lslice):
        start = lslice.start
        stop = lslice.stop if lslice.stop is not None else self.len

        if start < 0 or start >= self.len:
            raise IndexError('slice start index out of range')

        if start >= stop:
            raise IndexError('slice start must be less than slice end')

        if lslice.step and lslice.step != 1:
            raise ValueError('slice step must equal 1')

        values = []

        for i in range(start, min(self.len, stop)):
            values.append(self.__to_entry(self.db.dget('log', str(i))))

        return values

    def __get_item(self, index):
        if index < 0 or index >= self.len:
            raise IndexError('log index out of range')

        return self.__to_entry(self.db.dget('log', str(index)))

    def __init_log(self):
        if not self.db.exists('log'):
            self.db.dcreate('log')
        else:
            self.len = len(self.db.dkeys('log'))

    def __to_entry(self, attrs):
        entry = LogEntry()
        entry.__dict__ = attrs
        return entry

    class Iterator:
        def __init__(self, log):
            self.curr = 0
            self.log = log

        def __next__(self):
            if self.curr >= len(self.log):
                raise StopIteration
            entry = self.log[self.curr]
            self.curr += 1
            return entry


class PickleDbStateStore(MutableMapping):

    def __init__(self, db):
        self.db = db
        self.__init_store()

    def _keytransform(self, key):
        return key

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __setitem__(self, key, value):

        self.db.dadd('state', (key, value))
        self.db.dump()

    def __getitem__(self, key):
        if self.db.dexists('state', key):
            return self.db.dget('state', key)
        raise KeyError('key does not exist')

    def __init_store(self):
        if not self.db.exists('state'):
            self.db.dcreate('state')
