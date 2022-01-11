from ds_from_scratch.raft.task import *
from ds_from_scratch.raft.util import Logger


class RaftServer:
    def __init__(self, raft, executor, msg_gateway, network_interface):
        self.executor = executor
        self.raft = raft
        self.msg_gateway = msg_gateway
        self.network_interface = network_interface
        self.logger = Logger(address=raft.get_address())

    def get_network_interface(self):
        return self.network_interface

    def run(self):
        self.executor.submit(StartServerTask(raft=self.raft, executor=self.executor, msg_gateway=self.msg_gateway))

        while True:
            port = self.network_interface.port(port=0)
            msg = yield port.get()
            self.process_message(msg)

    def process_message(self, msg):
        operation = msg.body['operation']
        self.logger.info(
            "received message {op} from {sender}".format(op=operation, sender=msg.src_address.hostname))
        if operation == 'append_entries':
            self.executor.submit(AppendEntriesTask(
                raft=self.raft,
                msg_gateway=self.msg_gateway,
                executor=self.executor,
                msg=msg.body
            ))
