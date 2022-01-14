from ds_from_scratch.raft.state import Raft
from ds_from_scratch.raft.server import RaftServer
from ds_from_scratch.raft.util import Executor, MessageGateway
from ds_from_scratch.sim.core import *
from random import Random


class Simulation:
    def __init__(self, raft_by_address, network, env):
        self.env = env
        self.network = network
        self.raft_by_address = raft_by_address

    def disconnect_raft_nodes(self, *hostnames):
        for hostname in hostnames:
            self.network.disconnect_from_all(hostname)

    def connect_raft_nodes(self, *hostnames):
        for hostname in hostnames:
            self.network.connect_to_all(hostname)

    def get_raft_state_snapshots(self):
        snapshots = {}
        for address, raft in self.raft_by_address.items():
            snapshots[address] = raft.get_snapshot()
        return snapshots

    def get_raft_state_snapshot(self, hostname):
        return self.raft_by_address[hostname].get_snapshot()

    def run(self, until):
        while self.env.peek() != simpy.core.Infinity and self.env.now < until:
            self.env.step()


class SimulationBuilder:

    def __init__(self):
        self.env = Environment.create_instance()
        self.network = Network.create_instance()
        # start the network
        self.env.process(self.network.run())

        self.heartbeat_interval = 5
        self.raft_by_address = {}

    def build(self):
        return Simulation(env=self.env, network=self.network, raft_by_address=self.raft_by_address)

    def with_heartbeat_interval(self, interval):
        self.heartbeat_interval = interval

    def with_raft_node(self, hostname, role, current_term=0, prng=Random()):
        network_interface = NetworkInterface.create_instance(hostname)
        self.env.process(network_interface.listen())

        simulation_executor = SimulationExecutor()
        self.env.process(simulation_executor.run())
        executor = Executor(executor=simulation_executor)

        raft = Raft(address=hostname,
                    current_term=current_term,
                    role=role,
                    heartbeat_interval=self.heartbeat_interval,
                    prng=prng)

        self.raft_by_address[hostname] = raft

        server = RaftServer(executor=executor,
                            raft=raft,
                            msg_gateway=MessageGateway(),
                            network_interface=network_interface)
        self.env.process(server.run())

        self.network.add_server(server)
