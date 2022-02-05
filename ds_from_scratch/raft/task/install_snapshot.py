from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.model.raft import Role
from ds_from_scratch.raft.util import Logger


class InstallSnapshotTask:

    def __init__(self, state, msg, msg_board, snapshot_builder, executor):
        self.snapshot_builder = snapshot_builder
        self.msg_board = msg_board
        self.msg = msg
        self.state = state
        self.executor = executor
        self.logger = Logger(self.state.get_address())

    def run(self):
        if self.leaders_term_is_stale() or self.leaders_log_stale():
            self.reject_snapshot()
            return

        if self.is_follower():
            self.heard_from_leader()
        else:
            self.become_follower()

        self.append_chunk_to_snapshot()

        if self.is_last_chunk():
            self.apply_snapshot()
            self.finished_receiving_snapshot()
        else:
            self.finished_receiving_chunk()

    def leaders_log_stale(self):
        return self.msg['senders_term'] < self.state.get_current_term()

    def leaders_term_is_stale(self):
        return self.state.get_current_term() > self.msg['senders_term']

    def reject_snapshot(self):
        self.logger.info("rejected snapshot chunk from {sender}".format(sender=self.msg['sender']))
        self.msg_board.send_install_snapshot_response(receiver=self.msg['sender'], ok=False)

    def is_follower(self):
        return self.state.get_role() == Role.FOLLOWER

    def heard_from_leader(self):
        self.state.heard_from_peer(peers_term=self.leaders_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def become_follower(self):
        self.state.become_follower(peers_term=self.leaders_term())
        self.executor.cancel(ElectionTask)
        self.executor.cancel(ReplicateEntriesTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def append_chunk_to_snapshot(self):
        self.snapshot_builder.append_chunk(data=self.msg['data'],
                                           offset=self.msg['offset'],
                                           last_term=self.msg['last_term'],
                                           last_index=self.msg['last_index'])

    def is_last_chunk(self):
        return self.msg['done']

    def finished_receiving_chunk(self):
        self.msg_board.send_install_snapshot_response(receiver=self.msg['sender'], ok=True)

    def finished_receiving_snapshot(self):
        self.msg_board.send_install_snapshot_response(receiver=self.msg['sender'],
                                                      ok=True,
                                                      params={'last_repl_index': self.msg['last_index']})

    def apply_snapshot(self):
        if self.snapshot_builder.pending():
            self.state.install_snapshot(self.snapshot_builder.build())

    def leaders_term(self):
        return self.msg['senders_term']
