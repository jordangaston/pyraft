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
            self.executor.submit(HeartbeatTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway)
                                 )
        elif role is Role.FOLLOWER:
            self.logger.info('is follower')
        # TODO add handling for followers and candidates


class HeartbeatTask:
    def __init__(self, raft, msg_gateway, executor):
        self.raft = raft
        self.msg_gateway = msg_gateway
        self.executor = executor
        pass

    def run(self):
        if self.raft.get_role() is not Role.LEADER:
            return

        self.msg_gateway.send_heartbeat(sender=self.raft)
        self.executor.schedule(task=HeartbeatTask(raft=self.raft, msg_gateway=self.msg_gateway, executor=self.executor),
                               delay=self.raft.get_heartbeat_interval())


class ProcessHeartbeatTask:
    def __init__(self, raft, msg, msg_gateway, executor):
        self.executor = executor
        self.msg_gateway = msg_gateway
        self.msg = msg
        self.raft = raft
        self.logger = Logger(address=raft.get_address())

    def run(self):
        self.logger.info('received heartbeat from {sender}'.format(sender=self.msg.sender))
