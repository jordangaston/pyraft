from ds_from_scratch.raft.util import Logger


class ElectionTask:
    def __init__(self, state, executor, msg_board):
        self.msg_board = msg_board
        self.executor = executor
        self.state = state
        self.logger = Logger(address=state.get_address())

    def run(self):
        self.logger.info('started election')
        self.state.start_election()
        self.msg_board.request_votes()
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )