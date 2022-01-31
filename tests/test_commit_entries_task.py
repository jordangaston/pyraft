from ds_from_scratch.raft.log import LogEntry
from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.task import CommitEntriesTask
from ds_from_scratch.raft.util import Role, Executor
from ds_from_scratch.sim.testing import MockStateMachine


def test_should_commit(mocker):
    state = RaftState(
        'raft_1',
        role=Role.LEADER, current_term=1,
        log=[LogEntry(term=1, index=1, body=1, uid='entry_1'),
             LogEntry(term=1, index=2, body=2, uid='entry_2'),
             LogEntry(term=1, index=3, body=3, uid='entry_3')]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 3)
    state.set_peers_last_repl_index('raft_4', 0)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    executor = Executor(executor=None)
    mocker.patch.object(executor, 'complete_pending')

    task = CommitEntriesTask(state, msg_board, executor)

    task.run()

    assert len(state_machine.get_payloads()) == 2
    assert 1 in state_machine.get_payloads()
    assert 2 in state_machine.get_payloads()
    assert state.get_last_commit_index() == 2
    assert state.get_last_applied_index() == 2
    executor.complete_pending.assert_has_calls(
        [mocker.call(task_uid='entry_1', task_result=1),
         mocker.call(task_uid='entry_2', task_result=2)],
        any_order=True
    )


def test_should_not_commit_without_quorum(mocker):
    state = RaftState(
        'raft_1',
        role=Role.LEADER,
        state_store={'current_term': 1},
        log=[LogEntry(term=1, index=1, body=1, uid=None), LogEntry(term=1, index=2, body=2, uid=None),
             LogEntry(term=1, index=3, body=3, uid=None)]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 0)
    state.set_peers_last_repl_index('raft_4', 0)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    executor = Executor(executor=None)
    mocker.patch.object(executor, 'complete_pending')

    task = CommitEntriesTask(state, msg_board, executor)

    task.run()

    assert len(state_machine.get_payloads()) == 0
    assert state.get_last_commit_index() == 0
    assert state.get_last_applied_index() == 0
    executor.complete_pending.assert_not_called()


def test_should_do_nothing_unless_leader(mocker):
    state = RaftState(
        'raft_1',
        role=Role.FOLLOWER,
        state_store={'current_term': 1},
        log=[LogEntry(term=1, index=1, body=1, uid=None), LogEntry(term=1, index=2, body=2, uid=None),
             LogEntry(term=1, index=3, body=3, uid=None)]
    )

    state.set_peers_last_repl_index('raft_2', 2)
    state.set_peers_last_repl_index('raft_3', 2)
    state.set_peers_last_repl_index('raft_4', 2)

    msg_board = MessageBoard(state)
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 4

    state_machine = MockStateMachine()
    state.subscribe(state_machine)

    executor = Executor(executor=None)
    mocker.patch.object(executor, 'complete_pending')

    task = CommitEntriesTask(state, msg_board, executor)

    task.run()

    assert len(state_machine.get_payloads()) == 0
    assert state.get_last_commit_index() == 0
    assert state.get_last_applied_index() == 0
    executor.complete_pending.assert_not_called()
