from ds_from_scratch.sim.core import NetworkInterface, Environment
from enum import Enum


class MessageGateway:

    def __init__(self):
        pass

    def send_heartbeat(self, sender):
        network_interface = NetworkInterface.get_instance(sender.get_address())
        for hostname in network_interface.get_hostnames():
            if hostname == network_interface.get_hostname():
                continue
            conn = network_interface.open_connection(src_port=0, dst_port=0, dst_hostname=hostname)
            conn.send({
                'operation': 'append_entries',
                'senders_term': sender.get_current_term(),
                'sender': sender.get_address(),
            })
            conn.close()


class Executor:

    def __init__(self, executor):
        self.executor = executor

    def submit(self, task):
        return self.executor.submit(fn=task.run)

    def schedule(self, task, delay):
        return self.executor.schedule(fn=task.run, delay=delay)


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
    FOLLOWER = 0
