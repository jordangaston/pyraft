from ds_from_scratch.raft.log import LogEntry
from ds_from_scratch.raft.task import AppendEntriesTask, ElectionTask, ReplicateEntriesTask
from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.util import Role, Executor, RingBufferRandom
from ds_from_scratch.raft.message_board import MessageBoard


def test_entries_rejected_with_stale_leader_term(mocker):
    state = RaftState(address='state_node_1', role=Role.FOLLOWER, current_term=5)
    msg_board = MessageBoard(raft_state=state)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        msg={'sender': 'state_node_2', 'senders_term': 1, 'entries': []}
    )

    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    msg_board.send_append_entries_response.assert_called_once_with(receiver='state_node_2', ok=False)


def test_request_rejected_with_stale_log_term(mocker):
    state = RaftState(address='state_node_1', role=Role.FOLLOWER, current_term=2,
                      log=[LogEntry(term=2, index=1, body=None)])
    msg_board = MessageBoard(raft_state=state)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        msg={'sender': 'state_node_2', 'senders_term': 3, 'exp_last_log_entry': {'term': 3, 'index': 2}, 'entries': []}
    )

    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    msg_board.send_append_entries_response.assert_called_once_with(receiver='state_node_2', ok=False)


def test_request_rejected_with_stale_log_index(mocker):
    state = RaftState(address='state_node_1', role=Role.FOLLOWER, current_term=2,
                      log=[LogEntry(term=2, index=1, body=None)])
    msg_board = MessageBoard(raft_state=state)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        msg={'sender': 'state_node_2', 'senders_term': 3, 'exp_last_log_entry': {'term': 2, 'index': 2}, 'entries': []}
    )

    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    msg_board.send_append_entries_response.assert_called_once_with(receiver='state_node_2', ok=False)


def test_is_candidate(mocker):
    append_entries_msg = {
        'sender': 'raft_node_2',
        'senders_term': 2,
        'exp_last_log_entry': {'term': 0, 'index': 0},
        'entries': [LogEntry(term=2, index=1, body='entry_1')]
    }

    state = RaftState(
        address='raft_node_1',
        role=Role.CANDIDATE,
        current_term=1,
        prng=RingBufferRandom([12]),
        log=[]
    )

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg=append_entries_msg
    )

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    assert state.get_role() == Role.FOLLOWER
    executor.cancel.assert_has_calls([mocker.call(ReplicateEntriesTask), mocker.call(ElectionTask)], any_order=True)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_board.send_append_entries_response.assert_called_once_with(receiver='raft_node_2', ok=True, last_repl_index=1)


def test_is_leader(mocker):
    append_entries_msg = {
        'sender': 'raft_node_2',
        'senders_term': 10,
        'exp_last_log_entry': {'term': 10, 'index': 1},
        'entries': [LogEntry(term=10, index=2, body='entry_2')]
    }

    state = RaftState(
        address='state_node_1',
        role=Role.LEADER,
        current_term=5,
        prng=RingBufferRandom([12]),
        log=[LogEntry(term=10, index=1, body='entry_1')]
    )

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg=append_entries_msg
    )

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    assert state.get_role() == Role.FOLLOWER
    executor.cancel.assert_has_calls([mocker.call(ReplicateEntriesTask), mocker.call(ElectionTask)], any_order=True)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_board.send_append_entries_response.assert_called_once_with(receiver='raft_node_2', ok=True, last_repl_index=2)


def test_is_follower(mocker):
    append_entries_msg = {
        'sender': 'raft_node_2',
        'senders_term': 10,
        'exp_last_log_entry': {'term': 10, 'index': 2},
        'entries': [LogEntry(term=10, index=3, body='entry_1')]
    }

    state = RaftState(
        address='state_node_1',
        role=Role.FOLLOWER,
        current_term=5,
        prng=RingBufferRandom([12]),
        log=[LogEntry(term=10, index=1, body='entry_1'), LogEntry(term=10, index=2, body='entry_1')]
    )

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg=append_entries_msg
    )

    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_board.send_append_entries_response.assert_called_once_with(receiver='raft_node_2', ok=True, last_repl_index=3)


def test_when_heartbeat(mocker):
    append_entries_msg = {
        'sender': 'raft_node_2',
        'senders_term': 10,
        'exp_last_log_entry': {'term': 10, 'index': 2},
        'entries': []
    }

    state = RaftState(
        address='state_node_1',
        role=Role.FOLLOWER,
        current_term=5,
        prng=RingBufferRandom([12]),
        log=[LogEntry(term=10, index=1, body='entry_1'), LogEntry(term=10, index=2, body='entry_1')]
    )

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg=append_entries_msg
    )

    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(msg_board, 'send_append_entries_response')

    task.run()

    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_board.send_append_entries_response.assert_called_once_with(receiver='raft_node_2', ok=True, last_repl_index=0)