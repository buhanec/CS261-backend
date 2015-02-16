import threading
from Queue import Queue
from multiprocessing import cpu_count


class Plugins(type):
    """ Plugin metaclass to keep track of plugins """

    _plugins = []
    _input = []
    _storage = []
    _query = []

    def __init__(cls, name, bases, attrs):
        if name not in ['Plugin', 'InputPlugin', 'StoragePlugin',
                        'QueryPlugin']:
            Plugins._plugins.append(cls)
            # this could probably be done with __bases__ but idk
            if issubclass(cls, InputPlugin):
                Plugins._input.append(cls)
                cls._type.append('input')
            if issubclass(cls, QueryPlugin):
                Plugins._query.append(cls)
                cls._type.append('query')
            if issubclass(cls, StoragePlugin):
                Plugins._storage.append(cls)
                cls._type.append('storage')

    @property
    def name(cls):
        return cls._name

    @property
    def burst(cls):
        return cls._burst

    @property
    def type(cls):
        return cls._type


class Base(object):
    """ For proper MRO with mixins """

    def __init__(self, *args):
        """ Since object is the super, and the constructor takes not arguments
        make sure that we pass none - using Base instead of object guarantees
        that we can safely keep passing arguments through super(...).__init__
        """
        super(Base, self).__init__()


class Plugin(Base):
    """ Abstract base for plugins """
    __metaclass__ = Plugins

    # constants
    STATUS_BASE_INIT = -2
    STATUS_MIXIN_INIT = -1
    STATUS_LOADED = 0
    STATUS_INUSE = 1
    STATUS_ERROR = 2
    STATUS_STOPPED = 3

    # Plugin 'info'
    _name = None
    _type = []

    def __init__(self):
        super(Plugin, self).__init__()
        self.status = Plugin.STATUS_BASE_INIT
        print('[Plugin] init')

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value >= Plugin.STATUS_BASE_INIT and value <= Plugin.STATUS_STOPPED:
            self._status = value
        else:
            raise Exception('Bad status')

    @status.setter
    def status(self, value):
        self._status = value  # TODO: check status

    def unload(self):
        """ Stops the plugin operation and cleans up """

    def reload(self, options):
        """ Restarts the plugin operation """


class InputPlugin(object):
    """ Input plugin mixin """
    __metaclass__ = Plugins

    def __init__(self):
        super(InputPlugin, self).__init__()
        self._status = Plugin.STATUS_MIXIN_INIT
        self._threads = {}
        print('[InputPlugin] init')

    @property
    def threads(self):
        return self._threads

    @threads.setter
    def threads(self, value):
        raise Exception("cannot do this")

    def start(self, storage):
        thread = threading.Thread(target=self.fetch, args=(storage,))
        thread.start()
        self._threads[thread.ident] = (thread, threading.Event())
        return thread.ident

    def stop(self, tid):
        self._threads[tid][1].set()
        self._threads[tid][0].join()
        del self._threads[tid]

    def fetch(self, storage, workers):
        pass

    def unload(self):
        for tid in self.threads.keys():
            self.stop(tid)
        print "[InputPlugin] unload"


class StoragePlugin(object):
    """ Storage plugin mixin """
    __metaclass__ = Plugins

    def __init__(self, queue_size=0, workers=cpu_count()):
        super(StoragePlugin, self).__init__()
        self._status = Plugin.STATUS_MIXIN_INIT
        self._q = Queue(queue_size)
        self._threads = []
        self._terminate = threading.Event()
        for i in range(workers):
            t = threading.Thread(target=self.worker)
            self._threads.append(t)
            t.start()

    def enqueue(self, data):
        """ Stores raw data into a queue that will be processed by workers """
        self._q.put(data)

    def store(self, data):
        """ Stores single entry data into storage """

    def burst_store(self, data):
        """ Stores multiple entry data into storage """
        for d in data:
            self.store(d)
            pass

    def worker(self):
        """ Worker function that gets data from the queue and stores it """
        while not self._terminate.isSet():
            data = self._q.get()
            if data is None:  # flush blocked threads
                self._q.task_done()
                break
            self.burst_store(data)
            self._q.task_done()

    def unload(self, wait=False):
        """ Terminate all threads """
        self._terminate.set()
        for thread in self._threads:
            self._q.put(None)
        self._q.join()
        for thread in self._threads:
            thread.join()
        print "[StoragePlugin] unload"


class QueryPlugin(object):
    """ Query plugin mixing

    TODO: define required functionality
    """
    __metaclass__ = Plugins

    def __init__(self):
        super(QueryPlugin, self).__init__()
        self._status = Plugin.STATUS_MIXIN_INIT

    def clusturs(self, options):
        pass

    def trader_trades(self, trader, options):
        pass

    def trader_comms(self, trader, options):
        pass

    def stock_trades(self, stock, options):
        pass
