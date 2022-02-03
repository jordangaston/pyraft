from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.commit_entries import CommitEntriesTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.util import Logger
from ds_from_scratch.raft.model.raft import Role


class AppendEntriesResponseTask:
    """
    Handles messages from peers after attempting to append entries. It's exact behavior depends on the nodes state.

    1. Messages are ignored unless the node is the leader
    2. If the entries were successfully appended update the last replicated entry field for the follower
    3. Otherwise update the next entry field for the follower.  The correct entry will be retransmitted once the heartbeat
       timer elapses.
    """

    def __init__(self, state, msg, executor, msg_board):
        self.msg = msg
        self.msg_board = msg_board
        self.executor = executor
        self.state = state
        self.logger = Logger(address=state.get_address())

    def run(self):
        if not self.is_leader():
            return

        if self.is_stale():
            self.become_follower()
            return

        if self.request_successful():
            self.entries_appended()
            CommitEntriesTask(state=self.state,
                              msg_board=self.msg_board,
                              executor=self.executor).run()
        else:
            self.entries_rejected()

    def entries_rejected(self):
        self.state.set_peers_last_repl_index(self.peer(), self.msg['last_repl_index'])
        self.state.set_peers_next_index(self.peer(), self.state.peers_next_index(self.peer()) - 1)

    def entries_appended(self):
        if self.msg['last_repl_index'] == 0:
            return

        self.state.set_peers_last_repl_index(self.peer(), self.msg['last_repl_index'])
        self.state.set_peers_next_index(self.peer(), self.msg['last_repl_index'] + 1)

    def become_follower(self):
        self.state.become_follower(peers_term=self.peers_term())
        self.executor.cancel(ReplicateEntriesTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def request_successful(self):
        return self.msg['ok']

    def is_stale(self):
        return self.state.get_current_term() < self.msg['senders_term']

    def is_leader(self):
        return self.state.get_role() == Role.LEADER

    def peers_term(self):
        return self.msg['senders_term']

    def peer(self):
        return self.msg['sender']
