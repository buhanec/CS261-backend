import threading
import logging
from Queue import Queue

logger = logging.getLogger('demosystem')


class Plugins(type):
    """ Plugin metaclass to keep track of plugins """

    _plugins = {}
    _types = {}

    def __init__(cls, name, bases, attrs):
        if name not in ['Plugin', 'InputPlugin', 'StoragePlugin',
                        'QueryPlugin']:
            Plugins._plugins[cls._name] = cls
            Plugins._types[cls] = []
            if InputPlugin in bases:
                cls._types[cls].append('input')
            if QueryPlugin in bases:
                cls._types[cls].append('query')
            if StoragePlugin in bases:
                cls._types[cls].append('storage')
            logger.info('[Plugins] Plugin class %s found', cls._name)

    @property
    def name(cls):
        return cls._name

    @property
    def type(cls):
        return cls._types[cls]


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
    STATUS_BASE = -2
    STATUS_MIXIN = -1
    STATUS_INIT = 0
    STATUS_INUSE = 1
    STATUS_COMPLETED = 2

    # Plugin 'info'
    _name = None
    _type = []

    def __init__(self, unloader=None):
        super(Plugin, self).__init__()
        self.status = Plugin.STATUS_BASE
        self.logger = logger
        self.unloader = unloader
        logger.info('[Plugin] init')

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value >= Plugin.STATUS_BASE and value <= Plugin.STATUS_COMPLETED:
            self._status = value
        else:
            logger.error('Bad status')

    def unload(self):
        """ Stops the plugin operation and cleans up """

    def reload(self, options):
        """ Restarts the plugin operation """


class InputPlugin(object):
    """ Input plugin mixin """
    __metaclass__ = Plugins

    def __init__(self, unloader=None):
        super(InputPlugin, self).__init__(unloader=unloader)
        self._status = Plugin.STATUS_MIXIN
        self._threads = {}
        self._sem = threading.Semaphore(1)
        logger.info('[InputPlugin] init')

    @property
    def threads(self):
        return {tid: self._threads[tid][2] for tid in self._threads}

    @property
    def callbacks(self):
        return [thread[2] for thread in self._threads.values()]

    @threads.setter
    def threads(self, value):
        raise Exception("cannot do this")

    def start(self, callbacks):
        """ Creates a producer thread for every callback """
        if type(callbacks) is not list:
            callbacks = [callbacks]
        tids = []
        for c in callbacks:
            self._sem.acquire()
            thread = threading.Thread(target=self.fetch, args=(c,))
            thread.start()
            self._threads[thread.ident] = (thread, threading.Event(), c)
            self._sem.release()
            tids.append(thread.ident)
        return tids

    def stop_thread(self, tid):
        """ Stops a specific producer thread """
        self._sem.acquire()
        self._threads[tid][1].set()
        self._threads[tid][0].join()
        del self._threads[tid]
        self._sem.release()

    def stop(self, tids):
        """ Stops all producer threads """
        for tid in tids:
            self.stop_thread(tid)

    def fetch(self, storage, workers):
        """ Performs data collection """

    def unload(self):
        """ Unloads the plugin and performs any cleaning up """
        for tid in self.threads.keys():
            self.stop(tid)
        logger.info("[InputPlugin] unloaded")

    def reload(self):
        """ Reloads the plugin """
        callbacks = self.callbacks
        self.stop(self.threads.keys())
        self.start(callbacks)
        logger.info("[InputPlugin] reloaded")


class StoragePlugin(object):
    """ Storage plugin mixin """
    __metaclass__ = Plugins

    def __init__(self, queue_size=0, workers=1, unloader=None):
        super(StoragePlugin, self).__init__(unloader=None)
        self._status = Plugin.STATUS_MIXIN
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
        # self._q.join() - blocks if terminate is set and kills thread
        for thread in self._threads:
            thread.join()
        logger.info("[StoragePlugin] unloaded")
        if self.unloader is not None:
            logger.info("[StoragePlugin] calling external unloader")
            self.unloader()


class QueryPlugin(object):
    """ Query plugin mixing

    TODO: define required functionality
    """
    __metaclass__ = Plugins

    def __init__(self, unloader=None):
        super(QueryPlugin, self).__init__(unloader=unloader)
        self._status = Plugin.STATUS_MIXIN

    def trades(self, query, number):
        pass

    def comms(self, query, number):
        pass

    def alerts(self, query, number):
        pass

    def stock(self, symbol):
        pass

    def trader(self, email):
        pass

    def alert(self, alertid):
        pass

    def plot_data(self, column1, column2):
        pass
