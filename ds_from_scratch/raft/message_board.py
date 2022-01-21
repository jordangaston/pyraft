from ds_from_scratch.sim.core import NetworkInterface


class MessageBoard:

    def __init__(self, raft_state):
        self.raft_state = raft_state

    def get_peers(self):
        return self.network_interface().get_hostnames()

    def get_peer_count(self):

        return len(self.network_interface().get_hostnames())

    def send_request_vote_response(self, receiver, ok):
        self.__send('request_vote_response', receiver, ok)

    def send_append_entries_response(self, receiver, ok):
        self.__send('append_entries_response', receiver, ok)

    def send_heartbeat(self, receiver=None):
        self.__broadcast('append_entries')

    def request_votes(self):
        params = {
            'senders_last_log_entry': {
                'term': self.raft_state.get_last_log_term(),
                'index': self.raft_state.get_last_log_index()
            }
        }
        self.__broadcast('request_vote', params=params)

    def append_entries(self, receiver):
        pass

    def __send(self, operation, receiver, ok):
        self.network_interface().send(
            msg={
                'operation': operation,
                'senders_term': self.raft_state.get_current_term(),
                'sender': self.raft_state.get_address(),
                'ok': ok
            },
            dst_hostname=receiver
        )

    def __broadcast(self, operation, params=None, params_by_hostname=None):
        if params is None:
            params = {}

        if params_by_hostname is None:
            params_by_hostname = {}

        for hostname in self.network_interface().get_hostnames():
            if hostname == self.raft_state.get_address():
                continue

            if hostname in params_by_hostname:
                params_for_hostname = params_by_hostname[hostname]
            else:
                params_for_hostname = {}

            self.network_interface().send(
                msg={
                    'operation': operation,
                    'senders_term': self.raft_state.get_current_term(),
                    'sender': self.raft_state.get_address(),
                    **params,
                    **params_for_hostname
                },
                dst_hostname=hostname
            )

    def network_interface(self):
        return NetworkInterface.get_instance(self.raft_state.get_address())