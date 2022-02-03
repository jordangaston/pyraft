from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.model.raft import Role
from ds_from_scratch.raft.util import Logger


class RequestVoteTask:
    """
    Handles vote requests from candidates

    requests are rejected if...
    - the candidates log is not at least as up to date
    - the candidates term is not at least as up to date
    """

    def __init__(self, state, executor, msg, msg_board):
        self.msg_board = msg_board
        self.executor = executor
        self.msg = msg
        self.state = state
        self.logger = Logger(self.state.get_address())

    def run(self):

        # if self.state.get_address() == 'raft_node_2' and self.logger.now() == 50:
        #     breakpoint()

        if self.peers_term_is_stale() or self.peers_log_is_stale():
            self.reject_request()
            return

        if self.is_follower():
            self.heard_from_peer()
        elif self.term_is_stale():
            self.become_follower()

        self.finish_voting()

    def reject_request(self):
        self.logger.info("rejected vote request from {sender}".format(sender=self.peers_address()))
        self.msg_board.send_request_vote_response(receiver=self.peers_address(), ok=False)

    def become_follower(self):
        self.state.become_follower(peers_term=self.peers_term())
        self.executor.cancel(ReplicateEntriesTask)
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def heard_from_peer(self):
        self.state.heard_from_peer(peers_term=self.peers_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def finish_voting(self):
        self.msg_board.send_request_vote_response(receiver=self.peers_address(), ok=self.state.vote())

    def term_is_stale(self):
        return self.state.get_current_term() < self.peers_term()

    def peers_term_is_stale(self):
        return self.state.get_current_term() > self.peers_term()

    def peers_log_is_stale(self):
        term, index = self.last_log_term_index()
        peers_term, peers_index = self.peers_last_log_term_index()
        if term > peers_term:
            return True
        elif term < peers_term:
            return False
        else:
            return index > peers_index

    def peers_last_log_term_index(self):
        if 'senders_last_log_entry' not in self.msg:
            return 0, 0
        last_log_entry = self.msg['senders_last_log_entry']
        return last_log_entry['term'], last_log_entry['index']

    def last_log_term_index(self):
        return self.state.last_term(), self.state.last_index()

    def peers_address(self):
        return self.msg['sender']

    def peers_term(self):
        return self.msg['senders_term']

    def is_follower(self):
        return self.state.get_role() == Role.FOLLOWER
