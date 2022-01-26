from ds_from_scratch.raft.task import ReplicateEntriesTask
from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.util import Role
from ds_from_scratch.raft.message_board import MessageBoard


def test_send_heartbeat_when_peer_up_to_date(mocker):
    state = RaftState(address='raft_node_1', role=Role.LEADER, current_term=5)
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
    state = RaftState(address='raft_node_1', role=Role.LEADER, current_term=5)
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

    mocker.patch.object(state, 'peers_next_index')
    state.peers_next_index.return_value = 2

    mocker.patch.object(state, 'peers_last_index')
    state.peers_last_repl_index.return_value = 1

    mocker.patch.object(state, 'heartbeat_interval')
    state.heartbeat_interval.return_value = 5

    mocker.patch.object(msg_board, 'append_entries')

    task.run()

    msg_board.append_entries.assert_called_once_with('raft_node_2')
