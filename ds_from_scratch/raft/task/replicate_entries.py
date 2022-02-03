from ds_from_scratch.raft.model.raft import Role
from ds_from_scratch.raft.util import Logger


class ReplicateEntriesTask:
    """
    Sends append_entries messages from the leader to all peers.
    Exact behavior depends on the current state of the leader

    1. It sends a heartbeat message (contains no entries) if...
       a. The follower is up to date

    2. Otherwise it sends all pending entries

    Each message includes...
    1. The leaders current term
    2. The leaders hostname
    3. The entries to append
    4. The last expected entry's term and index
    """

    def __init__(self, state, msg_board, executor, recursive=True):
        self.state = state
        self.msg_board = msg_board
        self.executor = executor
        self.recursive = recursive
        self.logger = Logger(self.state.get_address())

    def run(self):
        if self.state.get_role() is not Role.LEADER:
            return

        for peer in self.msg_board.get_peers():
            if peer == self.state.get_address():
                continue

            if self.peer_up_to_date(peer):
                self.send_heartbeat(peer)
            else:
                self.send_entries_or_snapshot(peer)

        if self.recursive:
            self.executor.schedule(
                task=ReplicateEntriesTask(state=self.state, msg_board=self.msg_board, executor=self.executor),
                delay=self.state.get_heartbeat_interval())

    def peer_up_to_date(self, peer):
        return self.state.next_index() == self.state.peers_next_index(peer)

    def send_heartbeat(self, peer):
        self.msg_board.send_heartbeat(peer)

    def send_entries_or_snapshot(self, peer):
        if self.state.should_send_snapshot(peer):
            self.msg_board.install_snapshot(peer)
        else:
            self.msg_board.append_entries(peer)
