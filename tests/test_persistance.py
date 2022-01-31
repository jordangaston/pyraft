from ds_from_scratch.raft.util import Role, RingBufferRandom
from ds_from_scratch.raft.log import PickleDbLog
from ds_from_scratch.raft.store import PickleDbStateStore
from tests.simulation_assertions import assert_simulation_state
import pickledb


def test_state_is_persistent_when_node_bounced(simulation_builder, tmp_path):
    db = pickledb.load(str(tmp_path / 'raft_node_1.db'), False)

    simulation_builder.with_raft_node(hostname='raft_node_1',
                                      role=Role.FOLLOWER,
                                      prng=RingBufferRandom([10]),
                                      state_store=PickleDbStateStore(db),
                                      log=PickleDbLog(db))

    simulation_builder.with_raft_node(hostname='raft_node_2', role=Role.FOLLOWER, prng=RingBufferRandom([15]))
    simulation_builder.with_raft_node(hostname='raft_node_3', role=Role.FOLLOWER, prng=RingBufferRandom([20]))
    simulation = simulation_builder.build()

    simulation.run(until=20)

    simulation.execute_cmd(
        hostname='raft_node_1',
        cmd='cmd_1',
        cmd_uid='cmd_id_1',
    )

    simulation.run(until=50)

    simulation.bounce_raft_node('raft_node_1')

    state = simulation.get_raft_state('raft_node_1')
    log_entries = state.get_entries()

    assert len(log_entries) == 1
    assert log_entries[0].get_uid() == 'cmd_id_1'
    assert_simulation_state(simulation, expectations={
        'raft_node_1': {
            'role': Role.FOLLOWER,
            'current_term': 1,
            'voted': True,
        }
    })
