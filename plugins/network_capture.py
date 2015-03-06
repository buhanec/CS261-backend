import socket
from system2.plugins import Plugin, InputPlugin
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
import threading


class NetworkCapture(InputPlugin, Plugin):
    """ Network stream input """

    _name = 'NetCap'

    def __init__(self, source, buffer_size=1024):
        super(NetworkCapture, self).__init__()
        self.source = source
        self.buffer_size = buffer_size
        self.status = Plugin.STATUS_INIT
        self.logger.info('[NetworkCapture] init')

    def fetch(self, callback, workers=cpu_count()):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pool = ThreadPool(workers)
        s.connect(self.source)
        raw = s.recv(self.buffer_size).splitlines()
        pool.apply_async(callback, ([l.split(",") for l in raw[1:]],))
        while not self._threads[threading.current_thread().ident][1].isSet():
            raw = s.recv(self.buffer_size).splitlines()
            pool.apply_async(callback, ([l.split(",") for l in raw],))
        s.close()
        pool.close()
        pool.join()
        pool = None
