from ds_from_scratch.sim.core import NetworkInterface


class MessageBoard:

    def __init__(self, raft_state):
        self.raft_state = raft_state

    def get_peers(self):
        return NetworkInterface.get_hostnames()

    def get_peer_count(self):
        return len(self.get_peers())

    def send_request_vote_response(self, receiver, ok):
        self.__send('request_vote_response', receiver, params={'ok': ok})

    def request_votes(self):
        params = {
            'senders_last_log_entry': {
                'term': self.raft_state.last_term(),
                'index': self.raft_state.last_index()
            }
        }
        self.__broadcast('request_vote', params=params)

    def send_append_entries_response(self, receiver, ok, last_repl_index=0):
        params = {
            'ok': ok,
            'last_repl_index': last_repl_index
        }
        self.__send('append_entries_response', receiver, params=params)

    def send_heartbeat(self, receiver):
        term, index = self.__previous_term_index(self.raft_state.peers_next_index(receiver))

        params = {
            'exp_last_log_entry': {
                'term': term,
                'index': index
            },
            'entries': []
        }

        self.__send('append_entries', receiver, params=params)

    def append_entries(self, receiver):
        next_index = self.raft_state.peers_next_index(receiver)
        entries = self.raft_state.slice_entries(next_index)
        term, index = self.__previous_term_index(next_index)

        params = {
            'exp_last_log_entry': {
                'term': term,
                'index': index
            },
            'entries': entries
        }

        self.__send('append_entries', receiver, params=params)

    def __previous_term_index(self, next_index):
        previous_entry = self.raft_state.get_entry(next_index - 1)

        term = 0
        index = 0

        if previous_entry:
            term = previous_entry.get_term()
            index = previous_entry.index()

        return term, index

    def __send(self, operation, receiver, params=None):
        if params is None:
            params = {}

        NetworkInterface.send_message(
            self.raft_state.get_address(),
            receiver,
            {
                'operation': operation,
                'senders_term': self.raft_state.get_current_term(),
                'sender': self.raft_state.get_address(),
                **params
            }
        )

    def __broadcast(self, operation, params=None, params_by_hostname=None):
        if params is None:
            params = {}

        if params_by_hostname is None:
            params_by_hostname = {}

        for hostname in NetworkInterface.get_hostnames():
            if hostname == self.raft_state.get_address():
                continue

            if hostname in params_by_hostname:
                params_for_hostname = params_by_hostname[hostname]
            else:
                params_for_hostname = {}

            NetworkInterface.send_message(
                self.raft_state.get_address(),
                hostname,
                {
                    'operation': operation,
                    'senders_term': self.raft_state.get_current_term(),
                    'sender': self.raft_state.get_address(),
                    **params,
                    **params_for_hostname
                }
            )
