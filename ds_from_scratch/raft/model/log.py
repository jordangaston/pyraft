class Log:

    def __init__(self, log_store):
        self.log_store = log_store
        self.commit_index = 0

    def length(self):
        return len(self.log_store)

    def clear(self):
        self.log_store.clear()

    def truncate_until(self, index):
        if not self.__index_in_log(index):
            return

        while True:
            entry = self.log_store[0]
            if entry is not None and entry.get_index() <= index:
                self.log_store.pop(0)
            else:
                break

    def truncate_committed(self):

        if len(self.log_store) == 0:  # log is empty
            return

        if self.commit_index == 0:  # nothing has been committed
            return

        first_remaining = self.__pos_of_index(self.commit_index + 1)  # commit index is inclusive

        if first_remaining is None:
            self.log_store.clear()  # truncate all items
        else:
            for i in range(0, first_remaining):
                self.log_store.pop(0)

    def append_entries(self, *entries):
        for entry in entries:
            if self.__index_in_log(entry.get_index()):
                self.log_store[self.__pos_of_index(entry.get_index())] = entry
            else:
                self.log_store.append(entry)

        last_entry = entries[len(entries) - 1]
        return last_entry.get_index()

    def commit_entries(self, next_commit_index):
        if not self.__index_in_log(next_commit_index):
            return []

        committed = []

        commit_start = self.__pos_of_index(self.commit_index) if self.__index_in_log(self.commit_index) else 0
        commit_end = self.__pos_of_index(next_commit_index) + 1

        for entry in self.log_store[commit_start:commit_end]:
            self.commit_index = entry.get_index()
            committed.append(entry)

        return committed

    def last_commit_index(self):
        return self.commit_index

    def get_entry(self, index):
        if self.__index_in_log(index):
            return self.log_store[self.__pos_of_index(index)]
        return None

    def last_commit_entry(self):
        if len(self.log_store) == 0:
            return None

        if self.commit_index == 0:
            return None

        return self.get_entry(self.commit_index)

    def copy(self):
        return Log(self.log_store)

    def all_entries(self):
        return self.log_store.copy()

    def slice_entries(self, index):
        if not self.__index_in_log(index):
            return []

        return self.log_store[self.__pos_of_index(index):]

    def next_index(self):
        return self.__last_log_index() + 1

    def last_term(self):
        if self.last_entry() is None:
            return 0
        return self.last_entry().get_term()

    def last_index(self):
        if self.last_entry() is None:
            return 0
        return self.last_entry().get_index()

    def last_entry(self):
        num_entries = len(self.log_store)
        if num_entries == 0:
            return None
        return self.log_store[num_entries - 1]

    def __index_in_log(self, index):
        pos = self.__pos_of_index(index)
        return pos is not None

    def __pos_of_index(self, index):
        first = self.__first_log_index()
        last = self.__last_log_index()

        if first == 0 or last == 0:
            return None

        if index < first or index > last:
            return None

        return 0 + (index - first)

    def __last_log_index(self):
        entry = self.last_entry()
        if entry is None:
            return 0
        return entry.get_index()

    def __first_log_index(self):
        if len(self.log_store) == 0:
            return 0
        return self.log_store[0].get_index()


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
