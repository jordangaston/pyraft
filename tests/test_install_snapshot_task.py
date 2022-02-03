from ds_from_scratch.raft.model.log import Log
from ds_from_scratch.raft.model.snapshot import SnapshotBuilder
from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.task.election import ElectionTask
from ds_from_scratch.raft.task.install_snapshot import InstallSnapshotTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.executor import Executor


def test_snapshot_with_stale_leader_term(mocker):
    state = Raft(address='state_node_1', role=Role.FOLLOWER, state_store={'current_term': 5}, log=Log([]))
    msg_board = MessageBoard(raft_state=state)
    snapshot_builder = SnapshotBuilder(state_store={})
    executor = Executor(executor=None)

    task = InstallSnapshotTask(
        state=state,
        msg_board=msg_board,
        snapshot_builder=snapshot_builder,
        msg={'sender': 'state_node_2', 'senders_term': 1},
        executor=executor
    )

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    task.run()

    msg_board.send_install_snapshot_response.assert_called_once_with(receiver='state_node_2', ok=False)


def test_heard_from_peer_when_follower(mocker):
    state = Raft(address='state_node_1', role=Role.FOLLOWER, state_store={'current_term': 5}, log=Log([]))
    msg_board = MessageBoard(raft_state=state)
    snapshot_builder = SnapshotBuilder(state_store={})
    executor = Executor(executor=None)

    task = InstallSnapshotTask(
        state=state,
        msg_board=msg_board,
        snapshot_builder=snapshot_builder,
        msg={'sender': 'state_node_2',
             'senders_term': 10,
             'last_term': 10,
             'last_index': 2,
             'data': 'data',
             'offset': 0,
             'done': False},
        executor=executor
    )

    mocker.patch.object(state, 'heard_from_peer')
    mocker.patch.object(state, 'next_election_timeout')

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')

    mocker.patch.object(snapshot_builder, 'append_chunk')

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    task.run()

    state.heard_from_peer.assert_called_once_with(peers_term=10)
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    snapshot_builder.append_chunk.assert_called_once_with(data='data', offset=0, last_term=10, last_index=2)
    msg_board.send_install_snapshot_response.assert_called_once_with(receiver='state_node_2', ok=True)


def test_becomes_follower_when_candidate(mocker):
    state = Raft(address='state_node_1',
                 role=Role.CANDIDATE,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    snapshot_builder = SnapshotBuilder(state_store={})
    executor = Executor(executor=None)

    task = InstallSnapshotTask(
        state=state,
        msg_board=msg_board,
        snapshot_builder=snapshot_builder,
        msg={'sender': 'state_node_2',
             'senders_term': 10,
             'last_term': 10,
             'last_index': 2,
             'data': 'data',
             'offset': 0,
             'done': False},
        executor=executor
    )

    mocker.patch.object(state, 'become_follower')
    mocker.patch.object(state, 'next_election_timeout')

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')

    mocker.patch.object(snapshot_builder, 'append_chunk')

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    task.run()

    state.become_follower.assert_called_once_with(peers_term=10)
    executor.cancel.assert_has_calls([mocker.call(ReplicateEntriesTask), mocker.call(ElectionTask)], any_order=True)
    executor.schedule.assert_called_once()
    snapshot_builder.append_chunk.assert_called_once_with(data='data', offset=0, last_term=10, last_index=2)
    msg_board.send_install_snapshot_response.assert_called_once_with(receiver='state_node_2', ok=True)


def test_becomes_follower_when_leader(mocker):
    state = Raft(address='state_node_1',
                 role=Role.LEADER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    snapshot_builder = SnapshotBuilder(state_store={})
    executor = Executor(executor=None)

    task = InstallSnapshotTask(
        state=state,
        msg_board=msg_board,
        snapshot_builder=snapshot_builder,
        msg={'sender': 'state_node_2',
             'senders_term': 10,
             'last_term': 10,
             'last_index': 2,
             'data': 'data',
             'offset': 0,
             'done': False},
        executor=executor
    )

    mocker.patch.object(state, 'become_follower')
    mocker.patch.object(state, 'next_election_timeout')

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    mocker.patch.object(snapshot_builder, 'append_chunk')

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    task.run()

    state.become_follower.assert_called_once_with(peers_term=10)
    executor.cancel.assert_has_calls([mocker.call(ReplicateEntriesTask), mocker.call(ElectionTask)], any_order=True)
    executor.schedule.assert_called_once()
    snapshot_builder.append_chunk.assert_called_once_with(data='data', offset=0, last_term=10, last_index=2)
    msg_board.send_install_snapshot_response.assert_called_once_with(receiver='state_node_2', ok=True)


def test_applies_snapshot_when_done(mocker):
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    snapshot_builder = SnapshotBuilder(state_store={})
    executor = Executor(executor=None)

    task = InstallSnapshotTask(
        state=state,
        msg_board=msg_board,
        snapshot_builder=snapshot_builder,
        msg={'sender': 'state_node_2',
             'senders_term': 10,
             'last_term': 10,
             'last_index': 2,
             'data': 'data',
             'offset': 0,
             'done': True},
        executor=executor
    )

    mocker.patch.object(state, 'heard_from_peer')
    mocker.patch.object(state, 'next_election_timeout')
    mocker.patch.object(state, 'install_snapshot')

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    mocker.patch.object(snapshot_builder, 'append_chunk')
    mocker.patch.object(snapshot_builder, 'build')
    snapshot_builder.build.return_value = 'snapshot'

    mocker.patch.object(msg_board, 'send_install_snapshot_response')

    task.run()

    snapshot_builder.append_chunk.assert_called_once_with(data='data', offset=0, last_term=10, last_index=2)
    state.install_snapshot.assert_called_once_with('snapshot')
    msg_board.send_install_snapshot_response.assert_called_once_with(receiver='state_node_2', ok=True)
