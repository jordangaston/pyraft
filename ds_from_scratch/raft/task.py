from ds_from_scratch.raft.util import Role
from ds_from_scratch.raft.util import Logger


class StartServerTask:
    def __init__(self, raft, executor, msg_gateway):
        self.executor = executor
        self.raft = raft
        self.msg_gateway = msg_gateway
        self.logger = Logger(address=raft.get_address())

    def run(self):
        role = self.raft.get_role()
        if role is Role.LEADER:
            self.logger.info('is leader')
            self.executor.submit(HeartbeatTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway))
        elif role is Role.FOLLOWER:
            self.logger.info('is follower')
            self.executor.schedule(
                ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
                self.raft.next_election_timeout()
            )
        # TODO add handling for followers and candidates


class HeartbeatTask:
    def __init__(self, raft, msg_gateway, executor):
        self.raft = raft
        self.msg_gateway = msg_gateway
        self.executor = executor

    def run(self):
        if self.raft.get_role() is not Role.LEADER:
            return

        self.msg_gateway.send_heartbeat(sender=self.raft)
        self.executor.schedule(task=HeartbeatTask(raft=self.raft, msg_gateway=self.msg_gateway, executor=self.executor),
                               delay=self.raft.get_heartbeat_interval())


class AppendEntriesTask:
    def __init__(self, raft, msg, msg_gateway, executor):
        self.executor = executor
        self.msg_gateway = msg_gateway
        self.msg = msg
        self.raft = raft
        self.logger = Logger(address=raft.get_address())

    def run(self):
        if self.is_leader_stale():
            self.reject_entries()
            return

        if self.is_follower():
            self.heard_from_leader()
        else:
            self.become_follower()

        self.finished_appending_entries()

    def is_leader_stale(self):
        return self.raft.get_current_term() > self.leaders_term()

    def reject_entries(self):
        self.msg_gateway.send_append_entries_response(sender=self.raft, receiver=self.leaders_address(), ok=False)

    def is_follower(self):
        return self.raft.get_role() == Role.FOLLOWER

    def become_follower(self):
        self.raft.become_follower(peers_term=self.leaders_term())
        self.executor.cancel(HeartbeatTask)
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
            delay=self.raft.next_election_timeout()
        )

    def heard_from_leader(self):
        self.raft.heard_from_peer(peers_term=self.leaders_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
            delay=self.raft.next_election_timeout()
        )

    def finished_appending_entries(self):
        self.msg_gateway.send_append_entries_response(sender=self.raft, receiver=self.leaders_address(), ok=True)

    def leaders_address(self):
        return self.msg['sender']

    def leaders_term(self):
        return self.msg['senders_term']


class ElectionTask:
    def __init__(self, raft, executor, msg_gateway):
        self.msg_gateway = msg_gateway
        self.executor = executor
        self.raft = raft

    def run(self):
        self.raft.start_election()
        self.msg_gateway.request_votes(sender=self.raft)
        self.executor.schedule(
            task=ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
            delay=self.raft.next_election_timeout()
        )


class RequestVoteTask:
    def __init__(self, raft, executor, msg, msg_gateway):
        self.msg_gateway = msg_gateway
        self.executor = executor
        self.msg = msg
        self.raft = raft

    def run(self):
        if self.is_peer_stale():
            self.reject_request()
            return

        if self.is_follower():
            self.heard_from_peer()
        elif self.is_stale():
            self.become_follower()

        self.finish_voting()

    def reject_request(self):
        self.msg_gateway.send_request_vote_response(sender=self.raft, receiver=self.peers_address(), ok=False)

    def become_follower(self):
        self.raft.become_follower(peers_term=self.peers_term())
        self.executor.cancel(HeartbeatTask)
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
            delay=self.raft.next_election_timeout()
        )

    def heard_from_peer(self):
        self.raft.heard_from_peer(peers_term=self.peers_term())
        self.executor.cancel(ElectionTask)
        self.executor.schedule(
            task=ElectionTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway),
            delay=self.raft.next_election_timeout()
        )

    def finish_voting(self):
        self.msg_gateway.send_request_vote_response(sender=self.raft,
                                                    receiver=self.peers_address(),
                                                    ok=self.raft.vote())

    def is_stale(self):
        return self.raft.get_current_term() < self.peers_term()

    def is_peer_stale(self):
        return self.raft.get_current_term() > self.peers_term()

    def peers_address(self):
        return self.msg['sender']

    def peers_term(self):
        return self.msg['senders_term']

    def is_follower(self):
        return self.raft.get_role() == Role.FOLLOWER
