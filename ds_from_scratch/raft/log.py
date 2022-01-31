class PickleDbLog:

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


class LogEntry:

    def __init__(self, body=None, term=None, index=None, uid=None):
        self.uid = uid
        self.index = index
        self.term = term
        self.body = body

    def get_uid(self):
        return self.uid

    def get_index(self):
        return self.index

    def get_term(self):
        return self.term

    def get_body(self):
        return self.body
