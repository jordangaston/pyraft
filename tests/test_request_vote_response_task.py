from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.task import ReplicateEntriesTask, ElectionTask, RequestVoteResponseTask
from ds_from_scratch.raft.util import Role, MessageBoard, Executor


def test_becomes_follower_when_stale(mocker):
    state = RaftState(address='state_node_1', role=Role.CANDIDATE, current_term=5)
    msg_board = MessageBoard(raft_state=state, network_interface=None)
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg={'sender': 'state_node_2', 'senders_term': 10}
    )

    mocker.patch.object(state, 'become_follower')
    mocker.patch.object(state, 'next_election_timeout')
    state.next_election_timeout.return_value = 12
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'send_request_vote_response')

    task.run()

    state.become_follower.assert_called_once_with(peers_term=10)
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12


def test_becomes_leader_when_has_quorum(mocker):
    state = RaftState(address='state_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_board = MessageBoard(raft_state=state, network_interface=None)
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg={'sender': 'state_node_2', 'senders_term': 5, 'ok': True}
    )

    mocker.patch.object(state, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 2

    task.run()

    state.become_leader.assert_called_once()
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ReplicateEntriesTask
    assert args['delay'] == 5


def test_remains_candidate_without_quorum(mocker):
    state = RaftState(address='state_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_board = MessageBoard(raft_state=state, network_interface=None)
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg={'sender': 'state_node_2', 'senders_term': 5, 'ok': True}
    )

    mocker.patch.object(state, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 3

    task.run()

    state.become_leader.assert_not_called()
    executor.schedule.assert_not_called()
    executor.cancel.assert_not_called()


def test_remains_candidate_without_vote(mocker):
    state = RaftState(address='state_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_board = MessageBoard(raft_state=state, network_interface=None)
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        state=state,
        msg_board=msg_board,
        executor=executor,
        msg={'sender': 'state_node_2', 'senders_term': 5, 'ok': False}
    )

    mocker.patch.object(state, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_board, 'get_peer_count')
    msg_board.get_peer_count.return_value = 3

    task.run()

    state.become_leader.assert_not_called()
    executor.schedule.assert_not_called()
    executor.cancel.assert_not_called()
