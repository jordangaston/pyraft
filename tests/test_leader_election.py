from ds_from_scratch.raft.util import RingBufferRandom
from ds_from_scratch.sim.testing import SimulationBuilder
from ds_from_scratch.raft.server import Role
from tests.simulation_assertions import assert_simulation_state


def test_initial_leader_election(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation = simulation_builder.build()

    simulation.run(until=50)

    assert_simulation_state(simulation, leader='raft_node_1', current_term=1)


def test_leader_recovers_after_election(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([200]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([200]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([200]))
    simulation_builder.with_raft_node(hostname='raft_node_4', role=Role.FOLLOWER, prng=RingBufferRandom([200]))
    simulation_builder.with_raft_node(hostname='raft_node_5', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation = simulation_builder.build()

    # run normally for a bit
    simulation.run(until=20)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)

    # disconnect the leader
    simulation.disconnect_raft_nodes('raft_node_1')

    simulation.run(until=30)

    assert_simulation_state(simulation, expectations={
        'raft_node_1': {'role': Role.LEADER, 'current_term': 0},
        'raft_node_2': {'role': Role.FOLLOWER, 'current_term': 1},
        'raft_node_3': {'role': Role.FOLLOWER, 'current_term': 1},
        'raft_node_4': {'role': Role.FOLLOWER, 'current_term': 1},
        'raft_node_5': {'role': Role.LEADER, 'current_term': 1}
    })

    simulation.connect_raft_nodes('raft_node_1')
    simulation.run(until=50)

    assert_simulation_state(simulation, leader='raft_node_5', current_term=1)


def test_leader_recovers_before_election(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([30]))
    simulation_builder.with_raft_node(hostname='raft_node_4', role=Role.FOLLOWER, prng=RingBufferRandom([25]))
    simulation_builder.with_raft_node(hostname='raft_node_5', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation = simulation_builder.build()

    simulation.run(until=10)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)

    simulation.disconnect_raft_nodes('raft_node_1')
    simulation.run(until=15)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)

    simulation.connect_raft_nodes('raft_node_1')
    simulation.run(until=50)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)


def test_leader_recovers_during_election(simulation_builder):
    """
    a leader that recovers during a subsequent election should
    - update it's term
    - become a follower

    0: simulation starts with node 1 as the leader
    5: node 1 sends a heartbeat
    10: node 1 dies
    20: node 5 starts the election
    20: node 1 recovers
    20: node 5 wins the election
    """

    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([30]))
    simulation_builder.with_raft_node(hostname='raft_node_4', role=Role.FOLLOWER, prng=RingBufferRandom([25]))
    simulation_builder.with_raft_node(hostname='raft_node_5', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation = simulation_builder.build()

    simulation.run(until=10)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)

    simulation.disconnect_raft_nodes('raft_node_1')
    simulation.run(until=20)

    simulation.connect_raft_nodes('raft_node_1')
    simulation.run(until=50)
    assert_simulation_state(simulation, leader='raft_node_5', current_term=1)


def test_follower_recovers_after_election_timeout(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([20]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([30]))
    simulation = simulation_builder.build()

    simulation.disconnect_raft_nodes('raft_node_3')
    simulation.run(until=35)

    simulation.connect_raft_nodes('raft_node_3')
    simulation.run(until=60)
    assert_simulation_state(simulation, leader='raft_node_2', current_term=1)


def test_follower_recovers_before_election_timeout(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([20]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([30]))
    simulation = simulation_builder.build()

    simulation.disconnect_raft_nodes('raft_node_3')
    simulation.run(until=10)

    simulation.connect_raft_nodes('raft_node_3')
    simulation.run(until=50)
    assert_simulation_state(simulation, leader='raft_node_1', current_term=0)


def test_split_vote_should_occur(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation = simulation_builder.build()

    simulation.run(until=100)
    assert_simulation_state(simulation, expectations={
        'raft_node_1': {'role': Role.CANDIDATE},
        'raft_node_2': {'role': Role.CANDIDATE},
        'raft_node_3': {'role': Role.CANDIDATE},
    })


def test_no_election_after_majority_fails(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.LEADER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation = simulation_builder.build()

    simulation.run(until=10)
    simulation.disconnect_raft_nodes('raft_node_1', 'raft_node_2')
    simulation.run(until=100)
    assert_simulation_state(simulation, expectations={
        'raft_node_3': {'role': Role.CANDIDATE},
    })
