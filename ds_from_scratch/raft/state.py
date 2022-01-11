from ds_from_scratch.raft.util import Role
from random import Random


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

    def heard_from_peer(self, peers_term):
        self.current_term = peers_term

    def next_election_timeout(self):
        return self.prng.randint(self.election_timeout_range[0], self.election_timeout_range[1])

    def become_follower(self, leaders_term):
        self.role = Role.FOLLOWER
        self.current_term = leaders_term

    def get_heartbeat_interval(self):
        return self.heartbeat_interval

    def get_address(self):
        return self.address

    def get_role(self):
        return self.role

    def get_current_term(self):
        return self.current_term
