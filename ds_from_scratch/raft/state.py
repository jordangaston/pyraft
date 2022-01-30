from ds_from_scratch.raft.util import Role
from random import Random
import math


class RaftState:
    def __init__(self,
                 address,
                 role,
                 log=[],
                 stateStore={},
                 current_term=0,
                 heartbeat_interval=5,
                 election_timeout_range=(10, 20),
                 prng=Random()):

        self.log = log
        self.heartbeat_interval = heartbeat_interval
        self.address = address
        self.role = role
        self.current_term = current_term
        self.election_timeout_range = election_timeout_range
        self.prng = prng
        self.votes = set()
        self.voted = False
        self.default_next_index = None
        self.next_index_by_hostname = {}
        self.last_index_by_hostname = {}
        self.last_commit_index = 0
        self.last_applied_index = 0
        self.subscriber = None

    def subscribe(self, subscriber):
        self.subscriber = subscriber

    def get_last_applied_index(self):
        return self.last_applied_index

    def get_last_commit_index(self):
        return self.last_commit_index

    def peers_last_repl_indices(self):
        return self.last_index_by_hostname.values()

    def set_peers_last_repl_index(self, peer, index):
        if peer in self.last_index_by_hostname and self.last_index_by_hostname[peer] > index:
            return
        self.last_index_by_hostname[peer] = index

    def peers_last_repl_index(self, peer):
        if peer in self.last_index_by_hostname:
            return self.last_index_by_hostname[peer]
        return 0

    def set_peers_next_index(self, peer, index):
        assert self.get_role() == Role.LEADER
        self.next_index_by_hostname[peer] = index

    def peers_next_index(self, peer):
        if peer in self.next_index_by_hostname:
            return self.next_index_by_hostname[peer]
        return self.default_next_index

    def next_index(self):
        return len(self.log) + 1

    def last_term(self):
        entry = self.__last_log_entry()
        if entry is None:
            return 0
        return entry.get_term()

    def last_index(self):
        entry = self.__last_log_entry()
        if entry is None:
            return 0
        return entry.get_index()

    def slice_entries(self, index):
        if index > len(self.log):
            return []

        return self.log[(index - 1):]

    def get_entry(self, index):
        if len(self.log) == 0 or index not in range(1, len(self.log) + 1):
            return None
        return self.log[index - 1]

    def get_entries(self):
        return self.log.copy()

    def append_entries(self, *entries):
        for entry in entries:
            if entry.get_index() > len(self.log):
                self.log.append(entry)
            else:
                self.log[entry.get_index() - 1] = entry
        last_entry = entries[len(entries) - 1]
        return last_entry.get_index()

    def commit_entries(self, next_commit_index):
        if next_commit_index > len(self.log):
            return

        results = {}

        commit_start = max(self.get_last_commit_index() - 1, 0)
        commit_end = min(next_commit_index, max(1, len(self.log)))

        for entry in self.log[commit_start:commit_end]:
            self.last_commit_index = entry.get_index()
            results[entry.get_uid()] = self.subscriber.apply(entry.get_body())
            self.last_applied_index = entry.get_index()  # mark the entry as applied

        return results

    def get_snapshot(self):
        return {
            'role': self.role,
            'current_term': self.current_term,
            'last_commit_index': self.last_commit_index,
            'last_applied_index': self.last_applied_index
        }

    def get_votes(self):
        return self.votes.copy()

    def got_vote(self, sender):
        self.votes.add(sender)

    def vote(self):
        if self.voted:
            return False
        self.voted = True
        return True

    def start_election(self):
        """
        from Candidate or Follower
        - election timeout is hit
        """
        self.__clear_candidate_state()  # in case it is the candidate
        self.__change_term(new_term=self.current_term + 1)
        self.role = Role.CANDIDATE
        self.vote()
        self.got_vote(self.get_address())  # vote for myself

    def heard_from_peer(self, peers_term):
        if self.current_term < peers_term:
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
        assert peers_term >= self.current_term  # a valid leaders term must be at least a big

        if self.current_term < peers_term:
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
        self.default_next_index = self.next_index()
        self.role = Role.LEADER

    def get_heartbeat_interval(self):
        return self.heartbeat_interval

    def get_address(self):
        return self.address

    def get_role(self):
        return self.role

    def get_current_term(self):
        return self.current_term

    def has_quorum(self, peer_count):
        quorum = math.ceil(peer_count / 2)
        return len(self.votes) >= quorum

    def __last_log_entry(self):
        num_entries = len(self.log)
        if num_entries == 0:
            return None
        return self.log[num_entries - 1]

    def __change_term(self, new_term):
        self.current_term = new_term
        self.voted = False

    def __clear_candidate_state(self):
        self.votes.clear()

    def __clear_leader_state(self):
        self.next_index_by_hostname.clear()
        self.last_index_by_hostname.clear()
        self.default_next_index = None
