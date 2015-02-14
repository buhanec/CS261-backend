from system2.plugins import Plugin, StoragePlugin


class PrintStorage(Plugin, StoragePlugin):
    """ Test """

    _name = 'Printing Storage'
    _burst = False

    def __init__(self, buffer_size=1024):
        super(PrintStorage, self).__init__()
        print('[PrintStorage] init')

    def store(self, data):
        print(data)
