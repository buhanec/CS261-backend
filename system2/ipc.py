import socket
import threading


class SocketServer(object):
    """ IPC socket server """

    def __init__(self, ip='localhost', port=51233, buffer_size=1024):
        super(SocketServer, self).__init__()
        self.addr = (ip, port)
        self.buff = buffer_size
        self.terminate = False

    def start(self):
        self.terminate = False
        self.worker = threading.Thread(target=self.listen)
        self.worker.daemon = True
        self.worker.start()

    def listen(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(self.addr)
        s.listen(1)
        while not self.terminate:
            conn, addr = s.accept()
            try:
                while not self.terminate:
                    data = conn.recv(self.buff)
                    if data:
                        conn.sendall(data)  # just echo back for now
                    else:
                        break
            finally:
                conn.close()
        s.close()

    def stop(self):
        self.terminate = True
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(self.addr)
        self.worker.join()


class SocketClient(object):
    """ IPC socket client """

    def __init__(self, ip='localhost', port=51233, buffer_size=1024):
        super(SocketClient, self).__init__()
        self.addr = (ip, port)
        self.buff = buffer_size

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(self.addr)

    def message(self, message):
        self.s.send(message)
        return self.s.recv(self.buff)

    def disconnect(self):
        self.s.close()
