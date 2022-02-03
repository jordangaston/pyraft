from ds_from_scratch.raft.model.log import LogEntry, Log
from ds_from_scratch.raft.model.raft import Raft, Role

from ds_from_scratch.raft.model.snapshot import SnapshotBuilder, Snapshot
from ds_from_scratch.sim.testing import MockStateMachine


class TestBecomeFollower:

    def test_when_candidate_and_term_doesnt_change(self):
        """
        - candidate -> follower
        - has_voted does not change because term has not changed
        - votes changes because the candidate is no longer campaigning
        """
        state = Raft(address='address_1', role=Role.CANDIDATE, state_store={'current_term': 5}, log=Log([]))
        state.vote()
        state.got_vote('address_1')
        state.become_follower(peers_term=5)

        assert state.get_current_term() == 5
        assert state.get_role() == Role.FOLLOWER
        assert len(state.get_votes()) == 0
        assert state.vote() is False

    def test_when_candidate_and_term_changes(self):
        """
         - candidate -> follower
         - has_voted changes because the term changed
         - votes changes because the candidate is no longer campaigning
        """
        state = Raft(address='address_1', role=Role.CANDIDATE, state_store={'current_term': 5}, log=Log([]))
        state.vote()
        state.got_vote('address_1')
        state.become_follower(peers_term=6)

        assert state.get_current_term() == 6
        assert state.get_role() == Role.FOLLOWER
        assert len(state.get_votes()) == 0
        assert state.vote() is True

    def test_when_leader_and_term_doesnt_change(self):
        """
        - candidate -> follower
        - leader_state is reset since the user is no longer the leader
        """
        state = Raft(address='address_1', role=Role.LEADER, state_store={'current_term': 5}, log=Log([]))
        state.vote()
        state.got_vote('address_1')
        state.become_follower(peers_term=5)

        assert state.get_current_term() == 5
        assert state.get_role() == Role.FOLLOWER
        assert len(state.get_votes()) == 0
        assert state.vote() is False

    def test_when_leader_and_term_changed(self):
        """
        - candidate -> follower
        - leader_state is reset since the user is no longer the leader
        """
        state = Raft(address='address_1', role=Role.LEADER, state_store={'current_term': 5}, log=Log([]))
        state.vote()
        state.got_vote('address_1')
        state.become_follower(peers_term=6)

        assert state.get_current_term() == 6
        assert state.get_role() == Role.FOLLOWER
        assert len(state.get_votes()) == 0
        assert state.vote() is True


def test_append_entries():
    state = Raft(address='address_1',
                 role=Role.FOLLOWER,
                 log=Log([LogEntry(term=2, index=1, body=None, uid=None)]))

    last_index = state.append_entries(LogEntry(term=3, index=1, body=None, uid=None),
                                      LogEntry(term=3, index=2, body=None, uid=None))

    assert last_index == 2
    assert len(state.get_entries()) == 2

    entries = state.get_entries()

    assert entries[0].get_term() == 3 and entries[0].get_index() == 1
    assert entries[1].get_term() == 3 and entries[1].get_index() == 2


def test_should_not_send_snapshot_when_up_to_date():
    state = Raft(address='address_1',
                 role=Role.FOLLOWER,
                 state_store={
                     'snapshot': {
                         'last_term': 2,
                         'last_index': 3,
                         'state': {}
                     }
                 },
                 log=Log([]))

    state.set_peers_next_index('address_2', 4)

    assert not state.should_send_snapshot('address_2')


def test_should_send_snapshot():
    state = Raft(address='address_1',
                 role=Role.FOLLOWER,
                 state_store={
                     'snapshot': {
                         'last_term': 2,
                         'last_index': 3,
                         'state': {}
                     }
                 },
                 log=Log([]))

    state.set_peers_next_index('address_2', 3)

    assert state.should_send_snapshot('address_2')


def test_snapshot_transfer_flow():
    snapshot = {
        'last_term': 2,
        'last_index': 3,
        'state': 'state'
    }  # 46 bytes

    snapshot_builder = SnapshotBuilder({})

    raft = Raft(address='address_1',
                role=Role.FOLLOWER,
                max_chunk_size=20,
                state_store={'snapshot': snapshot},
                log=Log([]))

    assert raft.next_snapshot_chunk_offset('address_2') == 0

    last_chunk, chunk = raft.next_snapshot_chunk('address_2')
    assert not last_chunk

    snapshot_builder.append_chunk(
        data=chunk,
        offset=0,
        last_term=2,
        last_index=3
    )

    raft.ack_snapshot_chunk('address_2')

    assert raft.next_snapshot_chunk_offset('address_2') == 20

    last_chunk, chunk = raft.next_snapshot_chunk('address_2')
    assert not last_chunk

    snapshot_builder.append_chunk(
        data=chunk,
        offset=20,
        last_index=3,
        last_term=2
    )

    raft.ack_snapshot_chunk('address_2')

    assert raft.next_snapshot_chunk_offset('address_2') == 40

    last_chunk, chunk = raft.next_snapshot_chunk('address_2')
    assert last_chunk

    snapshot_builder.append_chunk(
        data=chunk,
        offset=40,
        last_index=3,
        last_term=2
    )

    raft.ack_snapshot_chunk('address_2')

    snapshot = snapshot_builder.build()
    assert snapshot.last_term() == 2
    assert snapshot.last_index() == 3
    assert snapshot.get_state() == 'state'


def test_should_clear_log_when_snapshot_installed():
    snapshot = Snapshot(
        state_store={},
        snapshot={
            'last_term': 2,
            'last_index': 3,
            'state': [345]
        }
    )

    raft = Raft(
        address='address_1',
        role=Role.FOLLOWER,
        max_chunk_size=20,
        state_store={
            'snapshot': {
                'last_term': 1,
                'last_index': 1,
                'state': 'state'
            }
        },
        log=Log([LogEntry(term=2, index=2)])
    )

    state_machine = MockStateMachine([123])

    raft.subscribe(state_machine)

    raft.install_snapshot(snapshot)

    snapshot = raft.get_snapshot()
    assert snapshot.last_term() == 2
    assert snapshot.last_index() == 3
    assert len(raft.get_entries()) == 0
    assert state_machine.get_payloads() == [345]


def test_should_truncate_log_when_snapshot_installed():
    snapshot = Snapshot(
        state_store={},
        snapshot={
            'last_term': 2,
            'last_index': 3,
            'state': [345]
        }
    )

    raft = Raft(
        address='address_1',
        role=Role.FOLLOWER,
        max_chunk_size=20,
        state_store={
            'snapshot': {
                'last_term': 1,
                'last_index': 1,
                'state': 'state'
            }
        },
        log=Log([LogEntry(term=2, index=2), LogEntry(term=2, index=4)])
    )

    state_machine = MockStateMachine([123])

    raft.subscribe(state_machine)

    raft.install_snapshot(snapshot)

    snapshot = raft.get_snapshot()
    assert snapshot.last_term() == 2
    assert snapshot.last_index() == 3
    assert len(raft.get_entries()) == 1
    assert state_machine.get_payloads() == [345]
