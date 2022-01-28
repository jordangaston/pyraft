from ds_from_scratch.raft.util import RingBufferRandom
from ds_from_scratch.raft.server import Role
from tests.simulation_assertions import assert_simulation_state


def test_initial_leader_election(simulation_builder):
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
