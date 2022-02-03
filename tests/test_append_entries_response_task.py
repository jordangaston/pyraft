from ds_from_scratch.raft.message_board import MessageBoard
from ds_from_scratch.raft.model.log import Log
from ds_from_scratch.raft.model.raft import Raft, Role
from ds_from_scratch.raft.task.append_entries_response import AppendEntriesResponseTask
from ds_from_scratch.raft.executor import Executor


def test_becomes_follower_when_stale():
    state = Raft(address='address_1',
                 role=Role.FOLLOWER,
                 state_store={'current_term': 5},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesResponseTask(
        executor=executor,
        msg_board=msg_board,
        state=state,
        msg={'senders_term': 10, 'senders': 'address_2'}
    )

    task.run()

    assert state.get_role() == Role.FOLLOWER


def test_entries_accepted(mocker):
    state = Raft(address='address_1',
                 role=Role.LEADER,
                 state_store={'current_term': 10},
                 log=Log([]))

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesResponseTask(
        executor=executor,
        msg_board=msg_board,
        state=state,
        msg={'senders_term': 10, 'sender': 'address_2', 'ok': True, 'last_repl_index': 25}
    )

    # commit_entries_task = mocker.patch('ds_from_scratch.raft.task.CommitEntriesTask')

    task.run()

    assert state.peers_last_repl_index('address_2') == 25
    assert state.peers_next_index('address_2') == 26
    # assert commit_entries_task.run.assert_called_once()


def test_entries_rejected():
    state = Raft(address='address_1',
                 role=Role.LEADER,
                 state_store={'current_term': 10},
                 log=Log([]))

    state.set_peers_next_index('address_2', 25)

    msg_board = MessageBoard(raft_state=state)
    executor = Executor(executor=None)

    task = AppendEntriesResponseTask(
        executor=executor,
        msg_board=msg_board,
        state=state,
        msg={'senders_term': 10, 'sender': 'address_2', 'ok': False, 'last_repl_index': 0}
    )

    task.run()

    assert state.peers_last_repl_index('address_2') == 0
    assert state.peers_next_index('address_2') == 24
