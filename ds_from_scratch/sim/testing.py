from ds_from_scratch.raft.model.log import Log
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.node import RaftNode
from ds_from_scratch.raft.executor import Executor
from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.sim.core import *
from random import Random


class Simulation:
    def __init__(self, raft_by_address, network, env, state_machine_by_address, node_by_address):
        self.node_by_address = node_by_address
        self.env = env
        self.network = network
        self.raft_by_address = raft_by_address
        self.state_machine_by_address = state_machine_by_address

    def bounce_raft_node(self, hostname):
        node = self.node_by_address[hostname]
        current_state = node.state
        node.state = Raft(
            address=hostname,
            role=Role.FOLLOWER,
            heartbeat_interval=current_state.heartbeat_interval,
            election_timeout_range=current_state.election_timer.timeout_range,
            prng=current_state.election_timer.prng,
            state_store=current_state.state_store,
            log=current_state.log.copy()
        )
        self.raft_by_address[hostname] = node.state

    def get_raft_state(self, hostname):
        return self.raft_by_address[hostname]

    def get_state_machine(self, hostname):
        return self.state_machine_by_address[hostname]

    def execute_cmd(self, hostname, cmd_uid, cmd):
        raft = self.node_by_address[hostname]
        return raft.execute_command(cmd_uid=cmd_uid, cmd=cmd)

    def disconnect_raft_nodes(self, *hostnames):
        for hostname in hostnames:
            self.network.disconnect_from_all(hostname)

    def connect_raft_nodes(self, *hostnames):
        for hostname in hostnames:
            self.network.connect_to_all(hostname)

    def get_state_machine_snapshots(self):
        snapshots = {}
        for address, state_machine in self.state_machine_by_address.items():
            snapshots[address] = state_machine.get_payloads()
        return snapshots

    def get_raft_state_snapshots(self):
        snapshots = {}
        for address, raft in self.raft_by_address.items():
            snapshots[address] = raft.get_report()
        return snapshots

    def get_raft_state_snapshot(self, hostname):
        return self.raft_by_address[hostname].get_report()

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
        self.state_machine_by_address = {}
        self.node_by_address = {}

    def build(self):
        return Simulation(env=self.env,
                          network=self.network,
                          raft_by_address=self.raft_by_address,
                          state_machine_by_address=self.state_machine_by_address,
                          node_by_address=self.node_by_address)

    def with_heartbeat_interval(self, interval):
        self.heartbeat_interval = interval

    def with_raft_node(self,
                       hostname,
                       role,
                       current_term=0,
                       max_chunk_size=10,
                       save_snapshot=lambda x: False,
                       prng=Random(),
                       state_store=None,
                       log_store=None):
        if log_store is None:
            log_store = []

        if state_store is None:
            state_store = {}

        simulation_executor = SimulationExecutor()
        self.env.process(simulation_executor.run())
        executor = Executor(executor=simulation_executor)

        state_store['current_term'] = current_term
        state_store['voted'] = False

        state = Raft(address=hostname,
                     role=role,
                     heartbeat_interval=self.heartbeat_interval,
                     prng=prng,
                     max_chunk_size=max_chunk_size,
                     save_snapshot=save_snapshot,
                     log=Log(log_store),
                     state_store=state_store)

        self.raft_by_address[hostname] = state

        raft = RaftNode(executor=executor,
                        state=state,
                        msg_board=MessageBoard(raft_state=state))

        self.node_by_address[hostname] = raft

        state_machine = MockStateMachine()

        raft.subscribe(state_machine)

        self.state_machine_by_address[hostname] = state_machine

        network_interface = NetworkInterface.create_instance(raft)
        self.env.process(network_interface.listen())

        self.network.add_network_interface(network_interface)


class MockStateMachine:

    def __init__(self, payloads=None):
        if payloads is None:
            payloads = []

        self.payloads = payloads

    def get_payloads(self):
        return self.payloads

    def get_state(self):
        return self.payloads

    def set_state(self, state):
        self.payloads = state

    def apply(self, payload):
        self.payloads.append(payload)
        return payload


class RingBufferRandom:

    def __init__(self, seq):
        self.seq = seq
        self.i = 0

    def copy(self):
        return RingBufferRandom(self.seq)

    def randint(self, a, b):
        result = self.seq[self.i]
        if self.i < len(self.seq) - 1:
            self.i += 1
        else:
            self.i = 0
        return result
