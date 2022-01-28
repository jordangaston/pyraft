from ds_from_scratch.raft.log import LogEntry
from ds_from_scratch.raft.state import RaftState
from ds_from_scratch.raft.util import Role


class TestBecomeFollower:

    def test_when_candidate_and_term_doesnt_change(self):
        """
        - candidate -> follower
        - has_voted does not change because term has not changed
        - votes changes because the candidate is no longer campaigning
        """
        state = RaftState(address='address_1', role=Role.CANDIDATE, current_term=5)
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
        state = RaftState(address='address_1', role=Role.CANDIDATE, current_term=5)
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
        state = RaftState(address='address_1', role=Role.LEADER, current_term=5)
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
        state = RaftState(address='address_1', role=Role.LEADER, current_term=5)
        state.vote()
        state.got_vote('address_1')
        state.become_follower(peers_term=6)

        assert state.get_current_term() == 6
        assert state.get_role() == Role.FOLLOWER
        assert len(state.get_votes()) == 0
        assert state.vote() is True


def test_append_entries():
    state = RaftState(address='address_1',
                      role=Role.FOLLOWER,
                      log=[LogEntry(term=2, index=1, body=None, uid=None)])

    last_index = state.append_entries(LogEntry(term=3, index=1, body=None, uid=None),
                                      LogEntry(term=3, index=2, body=None, uid=None))

    assert last_index == 2
    assert len(state.get_entries()) == 2

    entries = state.get_entries()

    assert entries[0].get_term() == 3 and entries[0].get_index() == 1
    assert entries[1].get_term() == 3 and entries[1].get_index() == 2
