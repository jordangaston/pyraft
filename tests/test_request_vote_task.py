from ds_from_scratch.raft.task import RequestVoteTask, HeartbeatTask, ElectionTask
from ds_from_scratch.raft.state import Raft
from ds_from_scratch.raft.util import Role, Executor, MessageGateway
from ds_from_scratch.raft.log import LogEntry


def test_request_rejected_when_candidate_term_stale(mocker):
    raft = Raft(address='raft_node_1', role=Role.FOLLOWER, current_term=5)
    msg_gateway = MessageGateway()

    task = RequestVoteTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=None,
        msg={'sender': 'raft_node_2', 'senders_term': 1}
    )

    mocker.patch.object(msg_gateway, 'send_request_vote_response')

    task.run()

    msg_gateway.send_request_vote_response.assert_called_once_with(sender=raft, receiver='raft_node_2', ok=False)


def test_request_rejected_with_stale_log_term(mocker):
    raft = Raft(address='raft_node_1', role=Role.FOLLOWER, current_term=2, log=[LogEntry(term=2, index=2, body=None)])
    msg_gateway = MessageGateway()

    task = RequestVoteTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=None,
        msg={'sender': 'raft_node_2', 'senders_term': 3, 'senders_last_log_entry': {'term': 1, 'index': 2}}
    )

    mocker.patch.object(msg_gateway, 'send_request_vote_response')

    task.run()

    msg_gateway.send_request_vote_response.assert_called_once_with(sender=raft, receiver='raft_node_2', ok=False)


def test_request_rejected_with_stale_log_index(mocker):
    raft = Raft(address='raft_node_1', role=Role.FOLLOWER, current_term=2, log=[LogEntry(term=2, index=3, body=None)])
    msg_gateway = MessageGateway()

    task = RequestVoteTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=None,
        msg={'sender': 'raft_node_2', 'senders_term': 3, 'senders_last_log_entry': {'term': 2, 'index': 2}}
    )

    mocker.patch.object(msg_gateway, 'send_request_vote_response')

    task.run()

    msg_gateway.send_request_vote_response.assert_called_once_with(sender=raft, receiver='raft_node_2', ok=False)


def test_leader_becomes_follower_when_term_stale(mocker):
    raft = Raft(address='raft_node_1', role=Role.LEADER, current_term=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteTask(
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
    executor.cancel.assert_has_calls([mocker.call(HeartbeatTask), mocker.call(ElectionTask)], any_order=True)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_gateway.send_request_vote_response.assert_called_once_with(sender=raft, receiver='raft_node_2', ok=True)


def test_follower(mocker):
    raft = Raft(address='raft_node_1', role=Role.FOLLOWER, current_term=5)
    msg_gateway = MessageGateway()
    executor = Executor(executor=None)

    task = RequestVoteTask(
        raft=raft,
        msg_gateway=msg_gateway,
        executor=executor,
        msg={'sender': 'raft_node_2', 'senders_term': 10}
    )

    mocker.patch.object(raft, 'heard_from_peer')
    mocker.patch.object(raft, 'next_election_timeout')
    raft.next_election_timeout.return_value = 12
    mocker.patch.object(executor, 'cancel')
    mocker.patch.object(executor, 'schedule')
    mocker.patch.object(msg_gateway, 'send_request_vote_response')

    task.run()

    raft.heard_from_peer.assert_called_once_with(peers_term=10)
    executor.cancel.assert_called_once_with(ElectionTask)
    executor.schedule.assert_called_once()
    args = executor.schedule.call_args[1]
    assert type(args['task']) is ElectionTask
    assert args['delay'] == 12
    msg_gateway.send_request_vote_response.assert_called_once_with(sender=raft, receiver='raft_node_2', ok=True)
