from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.model.raft import Role


class RequestVoteResponseTask:
    """
    Handles a peers response after requesting a vote.
    """

    def __init__(self, state, executor, msg, msg_board):
        self.msg_board = msg_board
        self.executor = executor
        self.msg = msg
        self.state = state

    def run(self):
        if not self.is_candidate():
            return

        if self.is_stale():
            self.become_follower()
            return

        self.update_vote_count()

        if self.has_quorum():
            self.become_leader()

    def become_follower(self):
        self.state.become_follower(peers_term=self.peers_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def update_vote_count(self):
        if self.msg['ok']:
            self.state.got_vote(self.msg['sender'])

    def become_leader(self):
        self.state.become_leader()
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ReplicateEntriesTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.get_heartbeat_interval()
        )

    def has_quorum(self):
        return self.state.has_quorum(peer_count=self.msg_board.get_peer_count())

    def is_candidate(self):
        return self.state.get_role() == Role.CANDIDATE

    def is_stale(self):
        return self.state.get_current_term() < self.peers_term()

    def peers_term(self):
        return self.msg['senders_term']