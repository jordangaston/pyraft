from ds_from_scratch.raft.util import Logger
from ds_from_scratch.raft.model.raft import Role


class CommitEntriesTask:

    def __init__(self, state, msg_board, executor):
        self.msg_board = msg_board
        self.state = state
        self.executor = executor
        self.logger = Logger(address=state.get_address())

    def run(self):
        if not self.__is_leader():
            return

        curr_commit_index = self.state.get_last_commit_index()
        next_commit_index = self.state.last_index()  # the next commit index can be at most the last entries index
        repl_count = 1  # number of peers who have replicated a non-committed entry. Set to one to represent the leader.

        for last_repl_index in self.state.peers_last_repl_indices():
            if last_repl_index <= curr_commit_index:
                continue  # nothing new to commit
            repl_count += 1
            next_commit_index = min(last_repl_index, next_commit_index)

        if self.__should_commit_entries(repl_count):
            self.logger.info("committing entries up to {i}".format(i=next_commit_index))
            results = self.state.commit_entries(next_commit_index)
            for uid, result in results.items():
                self.executor.complete_pending(task_uid=uid, task_result=result)

    def __is_leader(self):
        return self.state.get_role() == Role.LEADER

    def __should_commit_entries(self, repl_count):
        return repl_count > (self.msg_board.get_peer_count() / 2)
