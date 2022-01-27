from ds_from_scratch.raft.util import Role


def assert_simulation_state(simulation, expectations=None, leader=None, current_term=None):
    if leader and current_term:
        for name, actual_state in simulation.get_raft_state_snapshots().items():
            role = Role.FOLLOWER
            if name is leader:
                role = Role.LEADER
            assert actual_state['role'] == role
            assert actual_state['current_term'] == current_term
    elif expectations:
        for name, expected_state in expectations.items():
            actual_state = simulation.get_raft_state_snapshot(name)
            if 'roles' in expected_state:
                assert actual_state['role'] in expected_state['roles']
            if 'role' in expected_state:
                assert actual_state['role'] == expected_state['role']
            if 'current_term' in expected_state:
                assert actual_state['current_term'] == expected_state['current_term']
