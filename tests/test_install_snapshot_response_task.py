from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.model.log import Log
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.task.install_snapshot_response import InstallSnapshotResponseTask
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask
from ds_from_scratch.raft.executor import Executor


def test_does_nothing_unless_leader(mocker):
    state = Raft(address='state_node_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = InstallSnapshotResponseTask(
        state=state,
        msg_board=msg_board,
        msg={'sender': 'state_node_2', 'senders_term': 1},
        executor=executor
    )

    mocker.patch.object(state, 'ack_snapshot_chunk')

    task.run()


def test_becomes_follower_when_term_stale(mocker):
    state = Raft(address='state_node_1',
                 role=Role.LEADER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = InstallSnapshotResponseTask(
        state=state,
        msg_board=msg_board,
        msg={'sender': 'state_node_2', 'senders_term': 10},
        executor=executor
    )

    mocker.patch.object(state, 'next_election_timeout')
    mocker.patch.object(state, 'become_follower')

    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')

    mocker.patch.object(state, 'ack_snapshot_chunk')

    task.run()

    executor.cancel.assert_called_once_with(ReplicateEntriesTask)
    executor.schedule.assert_called_once()
    state.become_follower.assert_called_once_with(peers_term=10)
    state.ack_snapshot_chunk.assert_not_called()


def test_ack_snapshot_chunk(mocker):
    state = Raft(address='raft_node_1',
                 role=Role.LEADER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = InstallSnapshotResponseTask(
        state=state,
        msg_board=msg_board,
        msg={'sender': 'raft_node_2', 'senders_term': 1},
        executor=executor
    )

    mocker.patch.object(state, 'ack_snapshot_chunk')

    task.run()

    state.ack_snapshot_chunk.assert_called_once_with(peer='raft_node_2')
