from collections.abc import MutableMapping


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

    def __getitem__(self, key):
        if self.db.dexists('state', key):
            return self.db.dget('state', key)
        raise KeyError('key does not exist')

    def __init_store(self):
        if not self.db.exists('state'):
            self.db.dcreate('state')
            self['current_term'] = 0
            self['voted'] = False
