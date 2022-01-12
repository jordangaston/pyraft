from ds_from_scratch.raft.util import Role
from random import Random
import math


class RaftState:
    def __init__(self, address,
                 role,
                 current_term=0,
                 heartbeat_interval=5,
                 election_timeout_range=(10, 20),
                 prng=Random()):
        self.heartbeat_interval = heartbeat_interval
        self.address = address
        self.role = role
        self.current_term = current_term
        self.election_timeout_range = election_timeout_range
        self.prng = prng
        self.votes = set()
        self.voted = False

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
