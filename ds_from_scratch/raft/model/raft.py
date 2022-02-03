import math
from enum import Enum
from random import Random

from ds_from_scratch.raft.model.election_timer import ElectionTimer
from ds_from_scratch.raft.model.follower import Follower
from ds_from_scratch.raft.model.snapshot import Snapshot


class Role(Enum):
    CANDIDATE = 0
    LEADER = 1
    FOLLOWER = 2


class Raft:
    def __init__(self,
                 address,
                 role,
                 log,
                 state_store=None,
                 save_snapshot=lambda log: False,
                 max_chunk_size=10,
                 heartbeat_interval=5,
                 election_timeout_range=(10, 20),
                 prng=Random()):

        if state_store is None:
            state_store = {}

        self.address = address
        self.election_timer = ElectionTimer(election_timeout_range, prng)
        self.followers = {}
        self.heartbeat_interval = heartbeat_interval
        self.last_applied_index = 0
        self.log = log
        self.role = role
        self.save_snapshot = save_snapshot
        self.max_chunk_size = max_chunk_size
        self.snapshot_transfer_offset = {}
        self.snapshot = Snapshot(state_store)
        self.state_store = state_store
        self.subscriber = None
        self.votes = set()

    def get_snapshot(self):
        return self.snapshot

    def should_send_snapshot(self, peer):
        if not self.snapshot.exists():
            return False

        follower = self.followers.get(peer, self.__create_follower())
        return follower.behind_snapshot(self.snapshot)

    def ack_snapshot_chunk(self, peer):
        last_offset = self.snapshot_transfer_offset.get(peer, 0)
        self.snapshot_transfer_offset[peer] += min(self.snapshot.get_chunk_size(last_offset, self.max_chunk_size),
                                                   self.max_chunk_size)

    def next_snapshot_chunk_offset(self, peer):
        return self.snapshot_transfer_offset.setdefault(peer, 0)

    def next_snapshot_chunk(self, peer):
        next_offset = self.snapshot_transfer_offset.setdefault(peer, 0)
        return self.snapshot.get_chunk(next_offset, self.max_chunk_size)

    def last_snapshot_term(self):
        return self.snapshot.last_term()

    def last_snapshot_index(self):
        return self.snapshot.last_index()

    def install_snapshot(self, snapshot):
        self.snapshot = snapshot

        if self.snapshot.fresher_than(self.log):
            self.log.clear()
        else:
            self.log.truncate_until(self.snapshot.last_index())

        self.subscriber.set_state(self.snapshot.get_state())

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
        follower = self.followers.setdefault(peer, self.__create_follower())
        follower.set_last_index(index)

    def peers_last_repl_index(self, peer):
        follower = self.followers.setdefault(peer, self.__create_follower())
        return follower.last_index()

    def set_peers_next_index(self, peer, index):
        follower = self.followers.setdefault(peer, self.__create_follower())
        follower.set_next_index(index)

    def peers_next_index(self, peer):
        follower = self.followers.setdefault(peer, self.__create_follower())
        return follower.next_index()

    def next_index(self):
        return self.log.next_index()

    def last_term(self):
        last_term = self.log.last_term()
        if last_term == 0:
            last_term = self.snapshot.last_term()
        return last_term

    def last_index(self):
        last_index = self.log.last_index()
        if last_index == 0:
            last_index = self.snapshot.last_index()
        return last_index

    def slice_entries(self, index):
        return self.log.slice_entries(index)

    def get_entry(self, index):
        return self.log.get_entry(index)

    def get_entries(self):
        return self.log.all_entries()

    def append_entries(self, *entries):
        last_index = self.log.append_entries(*entries)

        if self.save_snapshot(self.log):
            self.snapshot.create(self.log, self.subscriber.get_state())
            self.log.truncate_committed()
        return last_index

    def commit_entries(self, next_commit_index):

        results = {}

        for entry in self.log.commit_entries(next_commit_index):
            results[entry.get_uid()] = self.subscriber.apply(entry.get_body())
            self.last_applied_index = entry.get_index()  # mark the entry as applied

        return results

    def get_report(self):
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
        return self.election_timer.next_timeout()

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

    def __set_current_term(self, term):
        self.state_store['current_term'] = term

    def __get_voted(self):
        return self.state_store.get('voted', False)

    def __set_voted(self, has_voted):
        self.state_store['voted'] = has_voted

    def __create_follower(self):
        return Follower(self.next_index())
