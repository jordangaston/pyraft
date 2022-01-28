class Log:

    def __setitem__(self, key, value):
        pass

    def __getitem__(self, item):
        pass

    def __iter__(self):
        pass

    def append(self):
        pass


class LogEntry:

    def __init__(self, body, term, index):
        self.index = index
        self.term = term
        self.body = body

    def get_index(self):
        return self.index

    def get_term(self):
        return self.term

    def get_body(self):
        return self.body
