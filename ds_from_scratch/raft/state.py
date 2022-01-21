from ds_from_scratch.raft.util import Role
from random import Random
import math


class RaftState:
    def __init__(self,
                 address,
                 role,
                 log=[],
                 state_machine=None,
                 current_term=0,
                 heartbeat_interval=5,
                 election_timeout_range=(10, 20),
                 prng=Random()):
        self.state_machine = state_machine
        self.log = log
        self.heartbeat_interval = heartbeat_interval
        self.address = address
        self.role = role
        self.current_term = current_term
        self.election_timeout_range = election_timeout_range
        self.prng = prng
        self.votes = set()
        self.voted = False
        self.next_index_by_hostname = {}
        self.last_index_by_hostname = {}
        self.last_commit_index = 0
        self.last_applied_index = 0

    def peers_last_repl_index(self, peer):
        pass

    def peers_next_index(self, peer):
        pass

    def next_index(self):
        pass

    def get_last_log_term(self):
        entry = self.__last_log_entry()
        if not entry:
            return 0
        return entry.get_term()

    def get_last_log_index(self):
        entry = self.__last_log_entry()
        if not entry:
            return 0
        return entry.get_index()

    def append_entry(self, entry):
        pass

    def commit_entry(self, index):
        pass

    def get_snapshot(self):
        return {
            'role': self.role,
            'current_term': self.current_term,
        }

    def got_vote(self, sender):
        self.votes.add(sender)

    def vote(self):
        if self.voted:
            return False
        self.voted = True
        return True

    def start_election(self):
        self.__clear_election_state()
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.votes.add(self.get_address())
        self.voted = True

    def heard_from_peer(self, peers_term):
        self.current_term = peers_term

    def next_election_timeout(self):
        return self.prng.randint(self.election_timeout_range[0], self.election_timeout_range[1])

    def become_follower(self, peers_term):
        self.role = Role.FOLLOWER
        if self.current_term < peers_term:
            self.current_term = peers_term
            self.__clear_election_state()

    def become_leader(self):
        if self.role != Role.CANDIDATE:
            return
        self.role = Role.LEADER
        self.__clear_election_state()

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

    def __clear_election_state(self):
        self.votes.clear()
        self.voted = False

    def __last_log_entry(self):
        num_entries = len(self.log)
        if num_entries == 0:
            return None
        return self.log[num_entries - 1]
