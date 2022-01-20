from ds_from_scratch.sim.core import Environment, NetworkInterface
from enum import Enum


class RingBufferRandom:

    def __init__(self, seq):
        self.seq = seq
        self.i = 0

    def randint(self, a, b):
        result = self.seq[self.i]
        if self.i < len(self.seq) - 1:
            self.i += 1
        else:
            self.i = 0
        return result


class MessageBoard:

    def __init__(self, raft_state):
        self.raft_state = raft_state

    def get_peer_count(self):

        return len(self.network_interface().get_hostnames())

    def send_request_vote_response(self, receiver, ok):
        self.__send('request_vote_response', receiver, ok)

    def send_append_entries_response(self, receiver, ok):
        self.__send('append_entries_response', receiver, ok)

    def send_heartbeat(self):
        self.__broadcast('append_entries')

    def request_votes(self):
        params = {
            'senders_last_log_entry': {
                'term': self.raft_state.get_last_log_term(),
                'index': self.raft_state.get_last_log_index()
            }
        }
        self.__broadcast('request_vote', params=params)

    def append_entries(self, receiver):
        pass

    def __send(self, operation, receiver, ok):
        self.network_interface().send(
            msg={
                'operation': operation,
                'senders_term': self.raft_state.get_current_term(),
                'sender': self.raft_state.get_address(),
                'ok': ok
            },
            dst_hostname=receiver
        )

    def __broadcast(self, operation, params=None, params_by_hostname=None):
        if params is None:
            params = {}

        if params_by_hostname is None:
            params_by_hostname = {}

        for hostname in self.network_interface().get_hostnames():
            if hostname == self.raft_state.get_address():
                continue

            if hostname in params_by_hostname:
                params_for_hostname = params_by_hostname[hostname]
            else:
                params_for_hostname = {}

            self.network_interface().send(
                msg={
                    'operation': operation,
                    'senders_term': self.raft_state.get_current_term(),
                    'sender': self.raft_state.get_address(),
                    **params,
                    **params_for_hostname
                },
                dst_hostname=hostname
            )

    def network_interface(self):
        return NetworkInterface.get_instance(self.raft_state.get_address())


class Executor:

    def __init__(self, executor):
        self.executor = executor
        self.future_by_task = {}

    def submit(self, task):
        self.executor.submit(fn=task.run)

    def schedule(self, task, delay):
        future = self.executor.schedule(fn=task.run, delay=delay)
        self.future_by_task[type(task).__name__] = future

    def cancel(self, task_klass):
        if task_klass.__name__ in self.future_by_task:
            future = self.future_by_task[task_klass.__name__]
            return future.cancel()
        return False


class Logger:
    def __init__(self, address):
        self.address = address

    def info(self, msg):
        print("[{address}] {now} :- {msg}".format(address=self.address, now=self.now(), msg=msg))

    def now(self):
        env = Environment.get_instance()
        return env.now


class Role(Enum):
    CANDIDATE = 0
    LEADER = 1
    FOLLOWER = 2
