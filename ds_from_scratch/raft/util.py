from ds_from_scratch.sim.core import NetworkInterface, Environment
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


class MessageGateway:

    def __init__(self):
        pass

    def get_peer_count(self):
        return len(NetworkInterface.get_hostnames())

    def send_request_vote_response(self, sender, receiver, ok):
        self.__send('request_vote_response', sender, receiver, ok)

    def send_append_entries_response(self, sender, receiver, ok):
        self.__send('append_entries_response', sender, receiver, ok)

    def send_heartbeat(self, sender):
        self.__broadcast('append_entries', sender)

    def request_votes(self, sender):
        params = {'senders_last_log_entry': {'term': sender.get_last_log_term(), 'index': sender.get_last_log_entry()}}
        self.__broadcast('request_vote', sender, params=params)

    def __send(self, operation, sender, receiver, ok):
        network_interface = NetworkInterface.get_instance(sender.get_address())
        conn = network_interface.open_connection(src_port=0, dst_port=0, dst_hostname=receiver)

        conn.send({
            'operation': operation,
            'senders_term': sender.get_current_term(),
            'sender': sender.get_address(),
            'ok': ok
        })
        conn.close()

    def __broadcast(self, operation, sender, params=None, params_by_hostname=None):
        if params is None:
            params = {}

        if params_by_hostname is None:
            params_by_hostname = {}

        network_interface = NetworkInterface.get_instance(sender.get_address())
        for hostname in NetworkInterface.get_hostnames():
            if hostname == network_interface.get_hostname():
                continue

            if hostname in params_by_hostname:
                params_for_hostname = params_by_hostname[hostname]
            else:
                params_for_hostname = {}

            conn = network_interface.open_connection(src_port=0, dst_port=0, dst_hostname=hostname)

            conn.send({
                'operation': operation,
                'senders_term': sender.get_current_term(),
                'sender': sender.get_address(),
                **params,
                **params_for_hostname
            })
            conn.close()


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
