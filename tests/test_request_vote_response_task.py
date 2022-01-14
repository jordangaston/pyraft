from ds_from_scratch.raft.state import Raft
from ds_from_scratch.raft.task import RequestVoteTask, HeartbeatTask, ElectionTask, RequestVoteResponseTask
from ds_from_scratch.raft.util import Role, MessageGateway, Executor


def test_becomes_follower_when_stale(mocker):
    raft = Raft(address='raft_node_1', role=Role.CANDIDATE, current_term=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=executor,
        msg={'sender': 'raft_node_2', 'senders_term': 10}
    )

    mocker.patch.object(raft, 'become_follower')
    mocker.patch.object(raft, 'next_election_timeout')
    raft.next_election_timeout.return_value = 12
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_gateway, 'send_request_vote_response')

    task.run()

    raft.become_follower.assert_called_once_with(peers_term=10)
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12


def test_becomes_leader_when_has_quorum(mocker):
    raft = Raft(address='raft_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=executor,
        msg={'sender': 'raft_node_2', 'senders_term': 5, 'ok': True}
    )

    mocker.patch.object(raft, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_gateway, 'get_peer_count')
    msg_gateway.get_peer_count.return_value = 2

    task.run()

    raft.become_leader.assert_called_once()
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is HeartbeatTask
    assert args['delay'] == 5


def test_remains_candidate_without_quorum(mocker):
    raft = Raft(address='raft_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=executor,
        msg={'sender': 'raft_node_2', 'senders_term': 5, 'ok': True}
    )

    mocker.patch.object(raft, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_gateway, 'get_peer_count')
    msg_gateway.get_peer_count.return_value = 3

    task.run()

    raft.become_leader.assert_not_called()
    executor.schedule.assert_not_called()
    executor.cancel.assert_not_called()


def test_remains_candidate_without_vote(mocker):
    raft = Raft(address='raft_node_1', role=Role.CANDIDATE, current_term=10, heartbeat_interval=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteResponseTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=executor,
        msg={'sender': 'raft_node_2', 'senders_term': 5, 'ok': False}
    )

    mocker.patch.object(raft, 'become_leader')
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_gateway, 'get_peer_count')
    msg_gateway.get_peer_count.return_value = 3

    task.run()

    raft.become_leader.assert_not_called()
    executor.schedule.assert_not_called()
    executor.cancel.assert_not_called()
