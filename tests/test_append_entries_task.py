from ds_from_scratch.raft.model.log import LogEntry, Log
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.task.append_entries import AppendEntriesTask
from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.sim.testing import RingBufferRandom
from ds_from_scratch.raft.executor import Executor
from ds_from_scratch.raft.message_board import MessageBoard


def test_entries_rejected_with_stale_leader_term(mocker):
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 5},
                 log=Log([]))

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
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 2},
                 log=Log([LogEntry(term=2, index=1, body=None, uid=None)]))

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
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 2},
                 log=Log([LogEntry(term=2, index=1, body=None, uid=None)]))
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


def test_request_rejected_with_stale_snapshot_term(mocker):
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={
                     'current_term': 2,
                     'snapshot': {
                         'last_term': 1,
                         'last_index': 2,
                         'state': {}
                     }
                 },
                 log=Log([]),
                 )

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


def test_request_rejected_with_stale_snapshot_index(mocker):
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={
                     'current_term': 2,
                     'snapshot': {
                         'last_term': 3,
                         'last_index': 1,
                         'state': {}
                     }
                 },
                 log=Log([]),
                 )

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


def test_is_candidate(mocker):
    append_entries_msg = {
        'sender': 'raft_node_2',
        'senders_term': 2,
        'last_commit_index': 0,
        'exp_last_log_entry': {'term': 0, 'index': 0},
        'entries': [LogEntry(term=2, index=1, body='entry_1', uid=None)]
    }

    state = Raft(
        address='raft_node_1',
        role=Role.CANDIDATE,
        state_store={'current_term': 1},
        prng=RingBufferRandom([12]),
        log=Log([])
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
        'last_commit_index': 0,
        'exp_last_log_entry': {'term': 10, 'index': 1},
        'entries': [LogEntry(term=10, index=2, body='entry_2', uid=None)]
    }

    state = Raft(
        address='state_node_1',
        role=Role.LEADER,
        state_store={'current_term': 5},
        prng=RingBufferRandom([12]),
        log=Log([LogEntry(term=10, index=1, body='entry_1', uid=None)])
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
        'last_commit_index': 0,
        'exp_last_log_entry': {'term': 10, 'index': 2},
        'entries': [LogEntry(term=10, index=3, body='entry_1', uid=None)]
    }

    state = Raft(
        address='state_node_1',
        role=Role.FOLLOWER,
        state_store={'current_term': 5},
        prng=RingBufferRandom([12]),
        log=Log([LogEntry(term=10, index=1, body='entry_1', uid=None),
                 LogEntry(term=10, index=2, body='entry_1', uid=None)])
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
        'last_commit_index': 0,
        'exp_last_log_entry': {'term': 10, 'index': 2},
        'entries': []
    }

    state = Raft(
        address='state_node_1',
        role=Role.FOLLOWER,
        state_store={'current_term': 5},
        prng=RingBufferRandom([12]),
        log=Log([LogEntry(term=10, index=1, body='entry_1', uid=None),
                 LogEntry(term=10, index=2, body='entry_1', uid=None)])
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
