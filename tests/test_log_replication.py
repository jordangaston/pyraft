from ds_from_scratch.raft.model.raft import Role
from ds_from_scratch.sim.testing import RingBufferRandom
from tests.simulation_assertions import assert_simulation_state


def test_entry_is_commited_when_all_nodes_healthy(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation = simulation_builder.build()

    simulation.run(until=20)

    future = simulation.execute_cmd(
        hostname='raft_node_1',
        cmd='cmd_1',
        cmd_uid='cmd_id_1',
    )

    simulation.run(until=50)

    snapshot = simulation.get_raft_state_snapshots()

    assert future.result() == 'cmd_1'
    assert_simulation_state(simulation, expectations={
        'raft_node_1': {
            'role': Role.LEADER,
            'last_commit_index': 1,
            'last_applied_index': 1,
            'subscriber': ['cmd_1']
        },
        'raft_node_2': {
            'role': Role.FOLLOWER,
            'last_commit_index': 1,
            'last_applied_index': 1,
            'subscriber': ['cmd_1']
        },
        'raft_node_3': {
            'role': Role.FOLLOWER,
            'last_commit_index': 1,
            'last_applied_index': 1,
            'subscriber': ['cmd_1']
        },
    })


def test_entry_is_committed_when_majority_of_nodes_healthy(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_a', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_b', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation_builder.with_raft_node(hostname='raft_node_c', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation = simulation_builder.build()

    simulation.run(until=20)

    simulation.disconnect_raft_nodes('raft_node_c')

    future = simulation.execute_cmd(
        hostname='raft_node_a',
        cmd='cmd_a',
        cmd_uid='cmd_id_a',
    )

    simulation.run(until=50)

    assert future.result() == 'cmd_a'
    assert_simulation_state(simulation, expectations={
        'raft_node_a': {
            'role': Role.LEADER,
            'last_commit_index': 1,
            'last_applied_index': 1,
            'subscriber': ['cmd_a']
        },
        'raft_node_b': {
            'role': Role.FOLLOWER,
            'last_commit_index': 1,
            'last_applied_index': 1,
            'subscriber': ['cmd_a']
        },
        'raft_node_c': {
            'roles': [Role.FOLLOWER, Role.CANDIDATE],
            'last_commit_index': 0,
            'last_applied_index': 0,
            'subscriber': []
        },
    })


def test_entry_is_not_committed_when_majority_of_nodes_not_healthy(simulation_builder):
    simulation_builder.with_raft_node(hostname='raft_node_1', role=Role.FOLLOWER, prng=RingBufferRandom([10]))
    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation = simulation_builder.build()

    simulation.run(until=20)

    simulation.disconnect_raft_nodes('raft_node_2')
    simulation.disconnect_raft_nodes('raft_node_3')

    future = simulation.execute_cmd(
        hostname='raft_node_1',
        cmd='cmd_1',
        cmd_uid='cmd_id_1',
    )

    simulation.run(until=50)

    assert not future.done()
    assert_simulation_state(simulation, expectations={
        'raft_node_1': {
            'role': Role.LEADER,
            'last_commit_index': 0,
            'last_applied_index': 0,
            'subscriber': []
        },
        'raft_node_2': {
            'roles': [Role.FOLLOWER, Role.CANDIDATE],
            'last_commit_index': 0,
            'last_applied_index': 0,
            'subscriber': []
        },
        'raft_node_3': {
            'roles': [Role.FOLLOWER, Role.CANDIDATE],
            'last_commit_index': 0,
            'last_applied_index': 0,
            'subscriber': []
        },
    })
