import threading


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
    STATUS_INIT = -1
    STATUS_LOADED = 0
    STATUS_INUSE = 1
    STATUS_ERROR = 2

    # Plugin 'info'
    _name = None
    _burst = True
    _type = []

    def __init__(self):
        super(Plugin, self).__init__()
        self.status = Plugin.STATUS_INIT
        self.terminate = False

    @property
    def status(self):
        """ Status of the plugin """
        return self._status

    @status.setter
    def status(self, value):
        self._status = value  # TODO: check status

    @property
    def terminate(self):
        """ Flag to start terminating plugin """
        return self._terminating

    @terminate.setter
    def terminate(self, value):
        self._terminating = value  # TODO: check flag

    def unload(self):
        """ Stops the plugin operation """

    def reload(self, options):
        """ Restarts the plugin operation """


class InputPlugin(object):
    """ Input plugin mixin """
    __metaclass__ = Plugins

    def __init__(self):
        super(InputPlugin, self).__init__()
        self._threads = {}
        print('[InputPlugin] init')

    def start(self, storage):
        thread = threading.Thread(target=self.fetch, args=(storage,))
        thread.start()
        self._threads[thread.ident] = (thread, threading.Event())
        print("started id", thread.ident)
        return thread.ident

    def stop(self, tid):
        self._threads[tid][1].set()
        self._threads[tid][0].join()
        print("stopped id", tid)

    def fetch(self, callback):
        pass


class StoragePlugin(object):
    """ Storage plugin mixin """
    __metaclass__ = Plugins

    def __init__(self):
        super(StoragePlugin, self).__init__()

    def store(self, data):
        pass

    def burst_store(self, data):
        for d in data:
            print "doing"
            self.store(d)


class QueryPlugin(object):
    """ Query plugin mixing

    TODO: define required functionality
    """
    __metaclass__ = Plugins

    def __init__(self):
        super(QueryPlugin, self).__init__()

    def clusturs(self, options):
        pass

    def trader_trades(self, trader, options):
        pass

    def trader_comms(self, trader, options):
        pass

    def stock_trades(self, stock, options):
        pass
