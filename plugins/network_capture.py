import socket
from system2.plugins import Plugin, InputPlugin
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
import threading


class NetworkCapture(InputPlugin, Plugin):
    """ Network stream input """

    _name = 'Network Capture'

    def __init__(self, source, buffer_size=1024):
        super(NetworkCapture, self).__init__()
        self.source = source
        self.buffer_size = buffer_size
        self.status = Plugin.STATUS_LOADED
        print('[NetworkCapture] init')

    def fetch(self, storage, workers=cpu_count()):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.source)
        pool = ThreadPool(workers)
        raw = s.recv(self.buffer_size).splitlines()
        pool.apply_async(storage.enqueue, ([l.split(",") for l in raw[1:]],))
        while not self._threads[threading.current_thread().ident][1].isSet():
            raw = s.recv(self.buffer_size).splitlines()
            pool.apply_async(storage.enqueue, ([l.split(",") for l in raw],))
        pool.close()
        pool.join()
        pool = None
        s.close()
