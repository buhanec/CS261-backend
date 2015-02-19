import os
from system2.plugins import Plugin, InputPlugin, StoragePlugin
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
from threading import Semaphore


class FileInput(InputPlugin, Plugin):
    """ File input plugin """

    _name = 'File Reader'

    def __init__(self, source):
        super(FileInput, self).__init__()
        self.source = source
        self.logger.info('[FileInput] init')

    def fetch(self, callback):
        with open(self.source, "r") as f:
            pool = ThreadPool(cpu_count())

            def fn(cb, data):
                lambda cb, data: pool.apply_async(cb, (data,))
            fn(callback, [l.split(",") for l in f.read().splitlines()])
            pool.close()
            pool.join()
            pool = None


class FileStore(StoragePlugin, Plugin):
    """ File storage plugin """

    _name = 'File Writer'

    def __init__(self, storage):
        super(FileStore, self).__init__()
        self.storage = storage
        self.semaphore = Semaphore(1)
        self.logger.info('[FileStore] init')

    def store(self, trade):
        self.semaphore.acquire()
        with open(self.storage, "a") as f:
            f.write(','.join(trade) + os.linesep)
        self.semaphore.release()
