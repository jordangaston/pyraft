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

    def send_request_vote_response(self, sender, receiver, ok):
        self.__send('request_vote_response', sender, receiver, ok)

    def send_append_entries_response(self, sender, receiver, ok):
        self.__send('append_entries_response', sender, receiver, ok)

    def send_heartbeat(self, sender):
        self.__broadcast('append_entries', sender)

    def request_votes(self, sender):
        self.__broadcast('request_vote', sender)

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

    def __broadcast(self, operation, sender):
        network_interface = NetworkInterface.get_instance(sender.get_address())
        for hostname in network_interface.get_hostnames():
            if hostname == network_interface.get_hostname():
                continue
            conn = network_interface.open_connection(src_port=0, dst_port=0, dst_hostname=hostname)
            conn.send({
                'operation': operation,
                'senders_term': sender.get_current_term(),
                'sender': sender.get_address(),
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
        future = self.future_by_task[task_klass.__name__]
        return future.cancel()


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
