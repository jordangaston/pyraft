import simpy
import concurrent.futures


class Environment:
    instance = None

    @classmethod
    def create_instance(cls):
        cls.instance = simpy.Environment()
        return cls.instance

    @classmethod
    def get_instance(cls):
        if cls.instance is None:
            cls.instance = simpy.Environment()
        return cls.instance


class Network:
    instance = None

    @classmethod
    def create_instance(cls):
        cls.instance = Network(env=Environment.get_instance())
        return cls.instance

    @classmethod
    def get_instance(cls):
        if cls.instance is None:
            cls.instance = Network(Environment.get_instance())
        return cls.instance

    def __init__(self, env):
        self.queue = simpy.Store(env=env, capacity=simpy.core.Infinity)
        self.ni_by_hostname = {}
        self.connections = set()

    def send(self, msg):
        self.queue.put(msg)

    def are_connected(self, src, dst):
        return tuple(sorted([src, dst])) in self.connections

    def hostnames(self):
        return self.ni_by_hostname.keys()

    def add_network_interface(self, ni):
        self.ni_by_hostname[ni.get_local_hostname()] = ni
        self.connect_to_all(ni.get_local_hostname())

    def connect_to_all(self, hostname):
        for peer_hostname in self.ni_by_hostname.keys():
            if peer_hostname is hostname:
                continue
            connection = tuple(sorted([peer_hostname, hostname]))
            if connection in self.connections:
                continue
            self.connections.add(connection)

    def disconnect_from_all(self, hostname):
        for peer_hostname in self.ni_by_hostname.keys():
            if peer_hostname is hostname:
                continue
            connection = tuple(sorted([peer_hostname, hostname]))
            if connection in self.connections:
                self.connections.remove(connection)

    def run(self):
        while True:
            msg = yield self.queue.get()
            if self.are_connected(msg.src_hostname, msg.dst_hostname):
                ni = self.ni_by_hostname[msg.dst_hostname]
                ni.queue.put(msg)


class NetworkInterface:
    instances = {}

    @classmethod
    def create_instance(cls, raft):
        cls.instances[raft.get_hostname()] = NetworkInterface(Environment.get_instance(), raft)
        return cls.instances[raft.get_hostname()]

    @classmethod
    def get_instance(cls, hostname):
        return cls.instances[hostname]

    @classmethod
    def get_hostnames(cls):
        return Network.get_instance().hostnames()

    @classmethod
    def send(cls, src, dst, msg):
        interface = cls.get_instance(hostname=src)
        interface.send(msg=msg, dst_hostname=dst)

    def __init__(self, env, raft):
        self.queue = simpy.Store(env=env, capacity=simpy.core.Infinity)
        self.raft = raft

    def get_local_hostname(self):
        return self.raft.get_hostname()

    def send(self, msg, dst_hostname):
        network = Network.get_instance()
        network.send(Message(
            env=Environment.get_instance(),
            src_hostname=self.raft.get_hostname(),
            dst_hostname=dst_hostname,
            body=msg
        ))

    def listen(self):
        while True:
            msg = yield self.queue.get()
            self.raft.process_message(msg)


class Message(simpy.Event):

    def __init__(self, env, src_hostname, dst_hostname, body):
        super().__init__(env)
        self.src_hostname = src_hostname
        self.dst_hostname = dst_hostname
        self.body = body


class SimulationExecutor(concurrent.futures.Executor):

    def __init__(self):
        self.queue = simpy.Store(env=Environment.get_instance(), capacity=simpy.core.Infinity)
        self.running = False

    def submit(self, fn, *args, **kwargs):
        future = concurrent.futures.Future()
        self.queue.put(
            Task(
                fn=fn,
                args=args,
                kwargs=kwargs,
                future=future
            )
        )
        return future

    def schedule(self, fn, delay, *args, **kwargs):
        future = concurrent.futures.Future()

        def callback(*a):
            can_run = future.set_running_or_notify_cancel()
            if can_run:
                fn(*a)
                future.set_result(None)

        Timer(interval=delay, function=callback, args=args).start()

        return future

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        raise NotImplementedError('Map is not implemented')

    def shutdown(self, wait=True):
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            task = yield self.queue.get()
            task.run()


class Task:
    def __init__(self, fn, args, kwargs, future):
        self.fn = fn
        self.args = args
        self.fn = fn
        self.kwargs = kwargs
        self.future = future

    def run(self):
        self.fn(*self.args, **self.kwargs)
        self.future.set_result(None)


class Timer:

    def __init__(self, interval, function, args=None):
        if args is None:
            args = []
        self.env = Environment.get_instance()
        self.interval = interval
        self.function = function
        self.args = args
        self.process = None

    def start(self):
        self.process = self.env.process(self.run())

    def run(self):
        try:
            yield self.env.timeout(self.interval)
            self.function(*self.args)
        except RuntimeError:
            return  # already cancelled
        except simpy.Interrupt:
            return

    def cancel(self):
        if self.process is None or self.process.processed:
            return
        self.process.interrupt()
