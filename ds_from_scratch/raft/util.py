from ds_from_scratch.sim.core import Environment


class Logger:
    def __init__(self, address):
        self.address = address

    def info(self, msg):
        print("[{address}] {now} :- {msg}".format(address=self.address, now=self.now(), msg=msg))

    def now(self):
        env = Environment.get_instance()
        return env.now
