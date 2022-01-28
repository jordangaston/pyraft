from ds_from_scratch.raft.log import LogEntry
from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.task import CommitEntriesTask
from ds_from_scratch.raft.util import Role


class MockStateMachine:

    def __init__(self):
        self.payloads = []

    def get_payloads(self):
        return self.payloads

    def apply(self, payload):
        self.payloads.append(payload)


def test_should_commit(mocker):
    state = RaftState(
        'raft_1',
        role=Role.LEADER, current_term=1,
        log=[LogEntry(term=1, index=1, body=1), LogEntry(term=1, index=2, body=2), LogEntry(term=1, index=3, body=3)]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 3)
    state.set_peers_last_repl_index('raft_4', 0)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    task = CommitEntriesTask(state, msg_board)

    task.run()

    assert len(state_machine.get_payloads()) == 2
    assert 1 in state_machine.get_payloads()
    assert 2 in state_machine.get_payloads()
    assert state.get_last_commit_index() == 2
    assert state.get_last_applied_index() == 2


def test_should_not_commit_without_quorum(mocker):
    state = RaftState(
        'raft_1',
        role=Role.LEADER, current_term=1,
        log=[LogEntry(term=1, index=1, body=1), LogEntry(term=1, index=2, body=2), LogEntry(term=1, index=3, body=3)]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 0)
    state.set_peers_last_repl_index('raft_4', 0)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    task = CommitEntriesTask(state, msg_board)

    task.run()

    assert len(state_machine.get_payloads()) == 0
    assert state.get_last_commit_index() == 0
    assert state.get_last_applied_index() == 0


def test_should_do_nothing_unless_leader(mocker):
    state = RaftState(
        'raft_1',
        role=Role.FOLLOWER, current_term=1,
        log=[LogEntry(term=1, index=1, body=1), LogEntry(term=1, index=2, body=2), LogEntry(term=1, index=3, body=3)]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 2)
    state.set_peers_last_repl_index('raft_4', 2)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    task = CommitEntriesTask(state, msg_board)

    task.run()

    assert len(state_machine.get_payloads()) == 0
    assert state.get_last_commit_index() == 0
    assert state.get_last_applied_index() == 0
