from ds_from_scratch.raft.model.log import LogEntry
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.model.raft import Role


class AcceptCommandTask:
    """
    Appends an entry for the cmd to the leaders log
    and attempts to replicate it (and any other un-replicated entries) immediately
    """

    def __init__(self, state, cmd_uid, cmd, executor, msg_board):
        self.cmd_uid = cmd_uid
        self.cmd = cmd
        self.msg_board = msg_board
        self.state = state
        self.executor = executor

    def run(self):
        if not self.is_leader():
            return

        entry = LogEntry(
            index=self.state.next_index(),
            term=self.state.get_current_term(),
            body=self.cmd,
            uid=self.cmd_uid
        )

        self.state.append_entries(entry)

        replication_task = ReplicateEntriesTask(
            msg_board=self.msg_board,
            state=self.state,
            executor=self.executor,
            recursive=False
        )
        replication_task.run()

    def is_leader(self):
        return self.state.get_role() == Role.LEADER