import os
from system2.plugins import Plugin, InputPlugin, StoragePlugin
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
from threading import Semaphore


class FileInput(Plugin, InputPlugin):
    """ File input plugin """

    _name = 'File Reader'

    def __init__(self, source):
        super(FileInput, self).__init__()
        self.source = source
        print('[FileInput] init')

    def fetch(self, storage):
        with open(self.source, "r") as f:
            pool = ThreadPool(cpu_count())
            if type(storage).burst:
                fn = lambda cb, data: pool.apply_async(cb, (data,))
                callback = storage.burst_store
            else:
                fn = pool.map_async
                callback = storage.store
            fn(callback, [l.split(",") for l in f.read().splitlines()])
            pool.close()
            pool.join()
            pool = None


class FileStore(Plugin, StoragePlugin):
    """ File storage plugin """

    _name = 'File Writer'
    _burst = False

    def __init__(self, storage):
        super(FileStore, self).__init__()
        self.storage = storage
        self.semaphore = Semaphore(1)
        print('[FileStore] init')

    def store(self, trade):
        self.semaphore.acquire()
        self.status = Plugin.STATUS_INUSE
        with open(self.storage, "a") as f:
            f.write(','.join(trade) + os.linesep)
        self.status = Plugin.STATUS_LOADED
        self.semaphore.release()
