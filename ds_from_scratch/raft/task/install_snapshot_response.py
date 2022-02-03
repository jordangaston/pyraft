from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.model.raft import Role


class InstallSnapshotResponseTask:

    def __init__(self, state, msg_board, executor, msg):
        self.msg_board = msg_board
        self.executor = executor
        self.msg = msg
        self.state = state

    def run(self):
        if not self.is_leader():
            return

        if self.is_stale():
            self.become_follower()
            return

        self.ack_snapshot_chunk()

    def is_leader(self):
        return self.state.get_role() == Role.LEADER

    def is_stale(self):
        return self.state.get_current_term() < self.peers_term()

    def become_follower(self):
        self.state.become_follower(peers_term=self.peers_term())
        self.executor.cancel(ReplicateEntriesTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def ack_snapshot_chunk(self):
        self.state.ack_snapshot_chunk(peer=self.msg['sender'])

    def peers_term(self):
        return self.msg['senders_term']