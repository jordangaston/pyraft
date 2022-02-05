from ds_from_scratch.raft.task.accept_command import AcceptCommandTask
from ds_from_scratch.raft.task.append_entries import AppendEntriesTask
from ds_from_scratch.raft.task.append_entries_response import AppendEntriesResponseTask
from ds_from_scratch.raft.task.install_snapshot import InstallSnapshotTask
from ds_from_scratch.raft.task.install_snapshot_response import InstallSnapshotResponseTask
from ds_from_scratch.raft.task.request_vote import RequestVoteTask
from ds_from_scratch.raft.task.request_vote_response import RequestVoteResponseTask
from ds_from_scratch.raft.task.start_server import StartServerTask
from ds_from_scratch.raft.util import Logger


class RaftNode:
    """
    A Raft node implemented using the Actor pattern
    """

    def __init__(self, state, executor, msg_board, snapshot_builder):
        self.executor = executor
        self.state = state
        self.msg_board = msg_board
        self.snapshot_builder = snapshot_builder
        self.logger = Logger(address=state.get_address())

        self.executor.submit(StartServerTask(
            state=self.state,
            msg_board=self.msg_board,
            executor=self.executor,
        ))

    def subscribe(self, subscriber):
        self.state.subscribe(subscriber)

    def get_hostname(self):
        return self.state.get_address()

    def execute_command(self, cmd_uid, cmd):
        self.logger.info("executing cmd {id}".format(id=cmd_uid))
        return self.executor.submit(
            AcceptCommandTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                cmd=cmd,
                cmd_uid=cmd_uid
            ),
            task_uid=cmd_uid
        )

    def process_message(self, msg):
        operation = msg.body['operation']
        self.logger.info("received message {op} from {sender}".format(op=operation, sender=msg.src_hostname))
        if operation == 'append_entries':
            self.executor.submit(AppendEntriesTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body
            ))
        elif operation == 'append_entries_response':
            self.executor.submit(AppendEntriesResponseTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body
            ))
        elif operation == 'request_vote':
            self.executor.submit(RequestVoteTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body
            ))
        elif operation == 'request_vote_response':
            self.executor.submit(RequestVoteResponseTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body
            ))
        elif operation == 'install_snapshot':
            self.executor.submit(InstallSnapshotTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body,
                snapshot_builder=self.snapshot_builder
            ))
        elif operation == 'install_snapshot_response':
            self.executor.submit(InstallSnapshotResponseTask(
                state=self.state,
                msg_board=self.msg_board,
                executor=self.executor,
                msg=msg.body
            ))
