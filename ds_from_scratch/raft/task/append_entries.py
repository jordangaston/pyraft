from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.util import Logger
from ds_from_scratch.raft.model.raft import Role


class AppendEntriesTask:
    """
    Appends entries to the nodes log.

    1. Entries are rejected if...
      a. the nodes term is greater than the leaders term
      b. the log consistency check fails
    2. If the leaders term is greater than the nodes term it becomes a follower
    3. The index and term of the last replicated entry is always returned to the sender
    """

    def __init__(self, state, msg, msg_board, executor):
        self.executor = executor
        self.msg_board = msg_board
        self.msg = msg
        self.state = state
        self.logger = Logger(address=state.get_address())

    def run(self):
        if self.leaders_term_stale() or self.log_is_inconsistent():
            self.reject_entries()
            return

        if self.is_follower():
            self.heard_from_leader()
        else:
            self.become_follower()

        last_replicated_index = self.append_entries()

        if self.should_commit():
            self.commit_entries()

        self.finished_appending_entries(last_replicated_index)

    def log_is_inconsistent(self):

        expected_term, expected_index = self.exp_last_term_index()
        if expected_term == 0 and expected_index == 0:
            return False

        entry = self.state.get_entry(expected_index)

        if entry is None:
            snapshot = self.state.get_snapshot()
            return snapshot.last_term() != expected_term or snapshot.last_index() != expected_index
        else:
            return entry.get_term() != expected_term or entry.get_index() != expected_index

    def leaders_term_stale(self):
        return self.state.get_current_term() > self.leaders_term()

    def exp_last_term_index(self):
        if 'exp_last_log_entry' not in self.msg:
            return 0, 0

        entry = self.msg['exp_last_log_entry']
        return entry['term'], entry['index']

    def reject_entries(self):
        self.msg_board.send_append_entries_response(receiver=self.leaders_address(), ok=False)

    def is_follower(self):
        return self.state.get_role() == Role.FOLLOWER

    def append_entries(self):
        entries = self.msg['entries']
        if len(entries) == 0:
            return 0
        return self.state.append_entries(*self.msg['entries'])

    def become_follower(self):
        self.state.become_follower(peers_term=self.leaders_term())
        self.executor.cancel(ReplicateEntriesTask)
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def heard_from_leader(self):
        self.state.heard_from_peer(peers_term=self.leaders_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def finished_appending_entries(self, last_repl_index):
        self.msg_board.send_append_entries_response(receiver=self.leaders_address(),
                                                    ok=True,
                                                    last_repl_index=last_repl_index)

    def leaders_address(self):
        return self.msg['sender']

    def leaders_term(self):
        return self.msg['senders_term']

    def should_commit(self):
        return self.state.get_last_commit_index() < self.msg['last_commit_index']

    def commit_entries(self):
        self.state.commit_entries(self.msg['last_commit_index'])
