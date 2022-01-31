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
            if name not in expectations:
                continue

            actual_state = simulation.get_raft_state_snapshot(name)
            if 'roles' in expected_state:
                assert actual_state['role'] in expected_state['roles']
            if 'role' in expected_state:
                assert actual_state['role'] == expected_state['role']
            if 'current_term' in expected_state:
                assert actual_state['current_term'] == expected_state['current_term']
            if 'last_commit_index' in expected_state:
                assert actual_state['last_commit_index'] == expected_state['last_commit_index']
            if 'last_applied_index' in expected_state:
                assert actual_state['last_applied_index'] == expected_state['last_applied_index']
            if 'subscriber' in expected_state:
                assert simulation.get_state_machine(name).get_payloads() == expected_state['subscriber']
            if 'voted' in expected_state:
                assert actual_state['voted'] == expected_state['voted']
