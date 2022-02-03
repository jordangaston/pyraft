from ds_from_scratch.raft.util import Role
from random import Random
import math


class RaftState:
    def __init__(self,
                 address,
                 role,
                 log=[],
                 state_store={},
                 heartbeat_interval=5,
                 election_timeout_range=(10, 20),
                 prng=Random()):

        self.state_store = state_store
        self.log = Log(log)
        self.heartbeat_interval = heartbeat_interval
        self.address = address
        self.role = role
        self.election_timeout_range = election_timeout_range
        self.prng = prng
        self.votes = set()
        self.followers = {}
        self.default_next_index = None
        self.last_commit_index = 0
        self.last_applied_index = 0
        self.subscriber = None

    def should_send_snapshot(self):
        pass

    def ack_snapshot_chunk(self, peer):
        pass

    def install_snapshot(self, snapshot):
        pass

    def subscribe(self, subscriber):
        self.subscriber = subscriber

    def get_last_applied_index(self):
        return self.last_applied_index

    def get_last_commit_index(self):
        return self.log.last_commit_index()

    def peers_last_repl_indices(self):
        values = []
        for follower in self.followers.values():
            values.append(follower.last_index())
        return values

    def set_peers_last_repl_index(self, peer, index):
        follower = self.followers.get(peer, Follower())
        follower.set_last_index(index)
        self.followers[peer] = follower

    def peers_last_repl_index(self, peer):
        follower = self.followers.get(peer, Follower())
        self.followers[peer] = follower
        return follower.last_index()

    def set_peers_next_index(self, peer, index):
        assert self.get_role() == Role.LEADER
        follower = self.followers.get(peer, Follower())
        follower.set_next_index(index)
        self.followers[peer] = follower

    def peers_next_index(self, peer):
        follower = self.followers.get(peer, Follower())
        self.followers[peer] = follower
        return follower.next_index()

    def next_index(self):
        return self.log.next_index()

    def last_term(self):
        return self.log.last_term()

    def last_index(self):
        return self.log.last_index()

    def slice_entries(self, index):
        return self.log.slice_entries(index)

    def get_entry(self, index):
        return self.log.get_entry(index)

    def get_entries(self):
        return self.log.all_entries()

    def append_entries(self, *entries):
        return self.log.append_entries(*entries)

    def commit_entries(self, next_commit_index):

        results = {}

        for entry in self.log.commit_entries(next_commit_index):
            results[entry.get_uid()] = self.subscriber.apply(entry.get_body())
            self.last_applied_index = entry.get_index()  # mark the entry as applied

        return results

    def get_snapshot(self):
        return {
            'role': self.role,
            'current_term': self.get_current_term(),
            'last_commit_index': self.log.last_commit_index(),
            'last_applied_index': self.last_applied_index,
            'voted': self.__get_voted()
        }

    def get_votes(self):
        return self.votes.copy()

    def got_vote(self, sender):
        self.votes.add(sender)

    def vote(self):
        if self.__get_voted():
            return False
        self.__set_voted(True)
        return True

    def start_election(self):
        """
        from Candidate or Follower
        - election timeout is hit
        """
        self.__clear_candidate_state()  # in case it is the candidate
        self.__change_term(new_term=self.get_current_term() + 1)
        self.role = Role.CANDIDATE
        self.vote()
        self.got_vote(self.get_address())  # vote for myself

    def heard_from_peer(self, peers_term):
        if self.get_current_term() < peers_term:
            self.__change_term(new_term=peers_term)

    def next_election_timeout(self):
        return self.prng.randint(self.election_timeout_range[0], self.election_timeout_range[1])

    def become_follower(self, peers_term):
        """
        from Leader
        - term is stale

        from Candidate
        - term is stale
        - discovered valid leader
        """
        assert self.role != Role.FOLLOWER
        assert peers_term >= self.get_current_term()  # a valid leaders term must be at least a big

        if self.get_current_term() < peers_term:
            self.__change_term(new_term=peers_term)

        self.__clear_leader_state()
        self.__clear_candidate_state()

        self.role = Role.FOLLOWER

    def become_leader(self, run_assertions=True):
        """
        from Candidate
        - has received quorum
        """
        if run_assertions:
            assert self.role == Role.CANDIDATE

        self.__clear_candidate_state()
        self.default_next_index = self.log.next_index()
        self.role = Role.LEADER

    def get_heartbeat_interval(self):
        return self.heartbeat_interval

    def get_address(self):
        return self.address

    def get_role(self):
        return self.role

    def get_current_term(self):
        return self.state_store.get('current_term', 0)

    def has_quorum(self, peer_count):
        quorum = math.ceil(peer_count / 2)
        return len(self.votes) >= quorum

    def __change_term(self, new_term):
        self.__set_current_term(new_term)
        self.__set_voted(False)

    def __clear_candidate_state(self):
        self.votes.clear()

    def __clear_leader_state(self):
        self.followers.clear()
        self.default_next_index = None

    def __set_current_term(self, term):
        self.state_store['current_term'] = term

    def __get_voted(self):
        return self.state_store.get('voted', False)

    def __set_voted(self, has_voted):
        self.state_store['voted'] = has_voted


class Snapshot:
    @classmethod
    def create(cls, log, state):
        pass


class Follower:
    def __init__(self):
        self.next = 0
        self.last = 0

    def behind_snapshot(self, snapshot):
        pass

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


class Log:

    def __init__(self, log_store):
        self.log_store = log_store
        self.commit_index = 0

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

    def store(self):
        return self.log_store

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
