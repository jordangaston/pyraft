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

    def are_connected(self, src, dst):
        return tuple(sorted([src, dst])) in self.connections

    def hostnames(self):
        return self.ni_by_hostname.keys()

    def add_server(self, server):
        ni = server.get_network_interface()
        self.ni_by_hostname[ni.get_hostname()] = ni
        self.connect_to_all(ni.get_hostname())

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
            if self.are_connected(msg.src_address.hostname, msg.dst_address.hostname):
                ni = self.ni_by_hostname[msg.dst_address.hostname]
                ni.queue.put(msg)


class IncomingConnection:

    def __init__(self, msg):
        self.msg = msg

    def recv(self):
        return self.msg.body

    def send(self, msg):
        network = Network.get_instance()
        network.queue.put(Message(
            env=Environment.get_instance(),
            src_address=self.msg.dst_address,
            dst_address=self.msg.src_address,
            body=msg
        ))

    def close(self):
        pass


class OutgoingConnection:

    def __init__(self, port, src_address, dst_address):
        self.src_address = src_address
        self.dst_address = dst_address
        self.queue = port

    def recv(self):
        msg = yield self.queue.get()
        return msg.body

    def send(self, msg):
        network = Network.get_instance()
        network.queue.put(Message(
            env=Environment.get_instance(),
            src_address=self.src_address,
            dst_address=self.dst_address,
            body=msg
        ))

    def close(self):
        self.queue = None


class NetworkInterface:
    instances = {}

    @classmethod
    def get_hostnames(self):
        return Network.get_instance().hostnames()

    @classmethod
    def create_instance(cls, hostname):
        cls.instances[hostname] = NetworkInterface(env=Environment.get_instance(), hostname=hostname)
        return cls.instances[hostname]

    @classmethod
    def get_instance(cls, hostname):
        if hostname not in cls.instances:
            cls.instances[hostname] = NetworkInterface(env=Environment.get_instance(), hostname=hostname)
        return cls.instances[hostname]

    def __init__(self, env, hostname):
        self.hostname = hostname
        self.queue = simpy.Store(env=env, capacity=simpy.core.Infinity)
        self.ports = {
            0: simpy.Store(env=env, capacity=simpy.core.Infinity),
            1: simpy.Store(env=env, capacity=simpy.core.Infinity),
            2: simpy.Store(env=env, capacity=simpy.core.Infinity)
        }

    def get_hostname(self):
        return self.hostname

    def port(self, port):
        return self.ports[port]

    def open_connection(self, src_port, dst_port, dst_hostname):
        dst_address = Address(hostname=dst_hostname, port=dst_port)
        src_address = Address(hostname=self.hostname, port=src_port)
        return OutgoingConnection(port=self.ports[src_port], src_address=src_address, dst_address=dst_address)

    def listen(self):
        while True:
            msg = yield self.queue.get()
            port = self.ports[msg.dst_address.port]
            port.put(msg)


class Address:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port


class Message(simpy.Event):

    def __init__(self, env, src_address, dst_address, body):
        super().__init__(env)
        self.src_address = src_address
        self.dst_address = dst_address
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
