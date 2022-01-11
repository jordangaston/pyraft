from ds_from_scratch.sim.testing import SimulationBuilder
from ds_from_scratch.raft.server import Role

if __name__ == '__main__':
    simulation_builder = SimulationBuilder()
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.FOLLOWER)
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.LEADER)
    simulation = simulation_builder.build()

    simulation.run(until=100)
