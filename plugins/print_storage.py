from system2.plugins import Plugin, StoragePlugin
from random import randint
from time import sleep


class PrintStorage(StoragePlugin, Plugin):
    """ Test """

    _name = 'Printer'

    def __init__(self, unloader=None):
        super(PrintStorage, self).__init__(unloader=unloader)
        self._status = Plugin.STATUS_INIT
        self.logger.info('[PrintStorage] init')

    def store(self, data):
        print(data)
        pass
