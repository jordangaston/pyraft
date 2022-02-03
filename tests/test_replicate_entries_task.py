from ds_from_scratch.raft.model.log import Log
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.task.replicate_entries import ReplicateEntriesTask


def test_send_heartbeat_when_peer_up_to_date(mocker):
    state = Raft(address='raft_node_1',
                 role=Role.LEADER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)

    task = ReplicateEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        recursive=False
    )

    mocker.patch.object(msg_board, 'get_peers')
    msg_board.get_peers.return_value = ['raft_node_2']

    mocker.patch.object(state, 'next_index')
    state.next_index.return_value = 2

    mocker.patch.object(state, 'peers_next_index')
    state.peers_next_index.return_value = 2

    mocker.patch.object(state, 'heartbeat_interval')
    state.heartbeat_interval.return_value = 5

    mocker.patch.object(msg_board, 'send_heartbeat')

    task.run()

    msg_board.send_heartbeat.assert_called_once_with('raft_node_2')


def test_send_entries(mocker):
    state = Raft(address='raft_node_1',
                 role=Role.LEADER, state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)

    task = ReplicateEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        recursive=False
    )

    mocker.patch.object(msg_board, 'get_peers')
    msg_board.get_peers.return_value = ['raft_node_2']

    mocker.patch.object(state, 'next_index')
    state.next_index.return_value = 3

    mocker.patch.object(state, 'should_send_snapshot')
    state.should_send_snapshot.return_value = False

    mocker.patch.object(state, 'peers_next_index')
    state.peers_next_index.return_value = 2

    mocker.patch.object(state, 'peers_last_repl_index')
    state.peers_last_repl_index.return_value = 1

    mocker.patch.object(state, 'heartbeat_interval')
    state.heartbeat_interval.return_value = 5

    mocker.patch.object(msg_board, 'append_entries')

    task.run()

    msg_board.append_entries.assert_called_once_with('raft_node_2')


def test_install_snapshot(mocker):
    state = Raft(address='raft_node_1',
                 role=Role.LEADER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)

    task = ReplicateEntriesTask(
        state=state,
        msg_board=msg_board,
        executor=None,
        recursive=False
    )

    mocker.patch.object(msg_board, 'get_peers')
    msg_board.get_peers.return_value = ['raft_node_2']

    mocker.patch.object(state, 'next_index')
    state.next_index.return_value = 3

    mocker.patch.object(state, 'should_send_snapshot')
    state.should_send_snapshot.return_value = True

    mocker.patch.object(state, 'peers_next_index')
    state.peers_next_index.return_value = 2

    mocker.patch.object(state, 'peers_last_repl_index')
    state.peers_last_repl_index.return_value = 1

    mocker.patch.object(state, 'heartbeat_interval')
    state.heartbeat_interval.return_value = 5

    mocker.patch.object(msg_board, 'install_snapshot')

    task.run()

    msg_board.install_snapshot.assert_called_once_with('raft_node_2')
