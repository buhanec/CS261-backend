from system2.plugins import Plugin, StoragePlugin
import datetime
import pandas as pd


class PandasStorage(StoragePlugin, Plugin):
    """ SQL Alchemy thing """

    _name = 'Memory Storage'

    def __init__(self):
        super(PandasStorage, self).__init__()
        self.status = Plugin.STATUS_LOADED
        print('[PandasStorage] init')
