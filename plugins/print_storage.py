from system2.plugins import Plugin, StoragePlugin


class PrintStorage(StoragePlugin, Plugin):
    """ Test """

    _name = 'Printing Storage'

    def __init__(self, buffer_size=1024):
        super(PrintStorage, self).__init__()
        self._status = Plugin.STATUS_LOADED
        print('[PrintStorage] init')

    def store(self, data):
        print(data)
