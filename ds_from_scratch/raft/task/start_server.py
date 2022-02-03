from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.util import Logger
from ds_from_scratch.raft.model.raft import Role


class StartServerTask:
    """
    Initializes a raft node upon startup
    """

    def __init__(self, state, executor, msg_board):
        self.executor = executor
        self.state = state
        self.msg_board = msg_board
        self.logger = Logger(address=state.get_address())

    def run(self):
        role = self.state.get_role()
        if role is Role.LEADER:
            self.logger.info('is leader')
            self.state.become_leader(run_assertions=False)
            self.executor.submit(
                ReplicateEntriesTask(state=self.state, executor=self.executor, msg_board=self.msg_board))
        elif role is Role.FOLLOWER:
            self.logger.info('is follower')
            self.executor.schedule(
                ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
                self.state.next_election_timeout()
            )
        # TODO add handling for candidates
