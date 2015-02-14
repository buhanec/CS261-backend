import socket
from system2.plugins import Plugin, InputPlugin
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
import threading


class NetworkCapture(Plugin, InputPlugin):
    """ Network stream input """

    _name = 'Network Capture'

    def __init__(self, source, buffer_size=1024):
        super(NetworkCapture, self).__init__()
        self.source = source
        self.buffer_size = buffer_size
        print('[NetworkCaptureT] init')

    def fetch(self, storage):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.source)
        pool = ThreadPool(cpu_count())
        if type(storage).burst:
            fn = lambda cb, data: pool.apply_async(cb, (data,))
            callback = storage.burst_store
        else:
            fn = pool.map_async
            callback = storage.store
            raw = s.recv(self.buffer_size).splitlines()
        fn(callback, [l.split(",") for l in raw[1:]])
        while not self._threads[threading.current_thread().ident][1].isSet():
            raw = s.recv(self.buffer_size).splitlines()
            fn(callback, [l.split(",") for l in raw])
        pool.close()
        pool.join()
        pool = None
        s.close()
