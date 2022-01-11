class RaftState:
    def __init__(self, address, role, current_term, heartbeat_interval):
        self.heartbeat_interval = heartbeat_interval
        self.address = address
        self.role = role
        self.current_term = current_term

    def get_heartbeat_interval(self):
        return self.heartbeat_interval

    def get_address(self):
        return self.address

    def get_role(self):
        return self.role

    def get_current_term(self):
        return self.current_term
