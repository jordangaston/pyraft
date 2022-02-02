from ds_from_scratch.raft.util import Role
from ds_from_scratch.raft.util import Logger
from ds_from_scratch.raft.log import LogEntry


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

    def run(self):
        if self.state.get_role() is not Role.LEADER:
            return

        for peer in self.msg_board.get_peers():
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
        if self.state.should_send_snapshot():
            self.msg_board.install_snapshot(peer)
        else:
            self.msg_board.append_entries(peer)


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
            self.commit_entries(last_replicated_index)

        self.finished_appending_entries(last_replicated_index)

    def log_is_inconsistent(self):

        expected_term, expected_index = self.exp_last_term_index()
        if expected_term == 0 and expected_index == 0:
            return False

        entry = self.state.get_entry(expected_index)

        if entry is None:
            return True

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

    def commit_entries(self, last_repl_index):
        # if the leader begins to send new entries in batches, a follower can never assume that it's log is up to date.
        # therefore it should only commit those entries that it knows to be replicated.
        self.state.commit_entries(min(self.msg['last_commit_index'], last_repl_index))


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

    def run(self):
        if self.peers_term_is_stale() or self.peers_log_is_stale():
            self.reject_request()
            return

        if self.is_follower():
            self.heard_from_peer()
        elif self.term_is_stale():
            self.become_follower()

        self.finish_voting()

    def reject_request(self):
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

    def peers_latest_log_entry(self):
        pass

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
            return index > peers_term

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


class RequestVoteResponseTask:
    """
    Handles a peers response after requesting a vote.
    """

    def __init__(self, state, executor, msg, msg_board):
        self.msg_board = msg_board
        self.executor = executor
        self.msg = msg
        self.state = state

    def run(self):
        if not self.is_candidate():
            return

        if self.is_stale():
            self.become_follower()
            return

        self.update_vote_count()

        if self.has_quorum():
            self.become_leader()

    def become_follower(self):
        self.state.become_follower(peers_term=self.peers_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.next_election_timeout()
        )

    def update_vote_count(self):
        if self.msg['ok']:
            self.state.got_vote(self.msg['sender'])

    def become_leader(self):
        self.state.become_leader()
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ReplicateEntriesTask(state=self.state, executor=self.executor, msg_board=self.msg_board),
            delay=self.state.get_heartbeat_interval()
        )

    def has_quorum(self):
        return self.state.has_quorum(peer_count=self.msg_board.get_peer_count())

    def is_candidate(self):
        return self.state.get_role() == Role.CANDIDATE

    def is_stale(self):
        return self.state.get_current_term() < self.peers_term()

    def peers_term(self):
        return self.msg['senders_term']


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


class InstallSnapshotTask:

    def __init__(self, state, msg, msg_board, snapshot_builder, executor):
        self.snapshot_builder = snapshot_builder
        self.msg_board = msg_board
        self.msg = msg
        self.state = state
        self.executor = executor

    def run(self):

        if self.leaders_log_stale():
            self.reject_snapshot()
            return

        if self.is_follower():
            self.heard_from_leader()
        else:
            self.become_follower()

        self.append_chunk_to_snapshot()

        if self.is_last_chunk():
            self.apply_snapshot()

        self.finished_receiving_chunk()

    def leaders_log_stale(self):
        return self.msg['senders_term'] < self.state.get_current_term()

    def reject_snapshot(self):
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

    def apply_snapshot(self):
        self.state.install_snapshot(self.snapshot_builder.build())

    def leaders_term(self):
        return self.msg['senders_term']


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
