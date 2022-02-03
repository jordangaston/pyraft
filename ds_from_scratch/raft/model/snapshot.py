import json


class Snapshot:

    def __init__(self, state_store, snapshot=None):
        if snapshot is None:
            snapshot = {}

        self.snapshot = snapshot
        self.state_store = state_store

        if self.exists():
            self.__save(snapshot)
        else:
            self.__load()

    def fresher_than(self, log):
        if not self.exists():
            return False

        if self.last_term() < log.last_term():
            return False
        elif self.last_term() > log.last_term():
            return True
        else:
            return self.last_index() > log.last_index()

    def last_index(self):
        return self.snapshot.get('last_index', 0)

    def last_term(self):
        return self.snapshot.get('last_term', 0)

    def get_state(self):
        return self.snapshot.get('state', None)

    def exists(self):
        if not self.snapshot:
            return False
        return True

    def create(self, log, state):
        entry = log.last_commit_entry()

        if entry is None:  # nothing is committed
            return

        self.snapshot = {
            'last_term': entry.get_term(),
            'last_index': entry.get_index(),
            'state': state
        }

        self.state_store['snapshot'] = self.snapshot

    def get_chunk(self, offset, max_length):
        if offset < 0:
            raise ValueError('offset cannot be less than 0')

        snapshot_bytes = self.__bytes()

        if offset >= len(snapshot_bytes):
            return True, bytes('', 'utf-8')

        chunk_end = offset + max_length

        return chunk_end >= len(snapshot_bytes), snapshot_bytes[offset:chunk_end]

    def get_chunk_size(self, offset, max_length):
        _last_chunk, chunk = self.get_chunk(offset, max_length)
        return len(chunk)

    def __save(self, snapshot):
        self.state_store['snapshot'] = snapshot

    def __load(self):
        if 'snapshot' in self.state_store:
            self.snapshot = self.state_store['snapshot']

    def __bytes(self):
        return bytes(json.dumps(self.snapshot), 'utf-8')


class SnapshotBuilder:

    def __init__(self, state_store):
        self.state_store = state_store

    def append_chunk(self, data, offset, last_index, last_term):
        if self.is_snapshot_stale(last_term=last_term, last_index=last_index):
            self.state_store['incoming_snapshot_term'] = last_term
            self.state_store['incoming_snapshot_index'] = last_index
            self.state_store['incoming_snapshot_offset'] = offset

        if offset <= self.state_store.get('incoming_snapshot_offset', -1):
            return

        snapshot_byte_string = self.state_store.get('incoming_snapshot', "")
        snapshot_byte_string += data.decode("utf-8")
        self.state_store['incoming_snapshot'] = snapshot_byte_string
        self.state_store['incoming_snapshot_offset'] = offset

    def build(self):
        snapshot = json.loads(self.state_store['incoming_snapshot'])
        self.state_store["incoming_snapshot"] = ""
        self.state_store["incoming_snapshot_offset"] = -1
        return Snapshot(state_store=self.state_store, snapshot=snapshot)

    def is_snapshot_stale(self, last_term, last_index):
        curr_term = self.state_store.get('incoming_snapshot_term', 0)
        curr_index = self.state_store.get('incoming_snapshot_index', 0)

        if curr_term == 0 and curr_index == 0:
            return False

        if last_term > curr_term:
            return True
        elif last_term == curr_term:
            return last_index > curr_index
        else:
            return False
