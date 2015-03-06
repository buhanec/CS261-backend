import logger
import os
import imp
from .plugins import Plugins
from .exceptions import *


class TheSystem(object):
    """ The core class """

    def __init__(self):
        logger.logger.info('[TheSystem] init')
        # probably need to change this at some point
        self._plugins = {}
        self._available = Plugins._plugins
        self._threads = {
            'input': {},
            'storage': {},
            'query': {}
        }
        self._interface = None
        self._connections = {}
        self._plugin_id = 0
        self.find_plugins(["plugins"])

    @property
    def plugins(self):
        return self._available.keys()

    @property
    def loaded_plugins(self):
        return {k: self._plugins[k][0] for k in self._plugins}

    @property
    def loaded_input_plugins(self):
        return {k: self._plugins[k][0] for k in self._threads['input']}

    @property
    def loaded_storage_plugins(self):
        return {k: self._plugins[k][0] for k in self._threads['storage']}

    @property
    def loaded_query_plugins(self):
        return {k: self._plugins[k][0] for k in self._threads['query']}

    @property
    def connections(self):
        return self._connections

    @property
    def interface_id(self):
        return self._interface

    @property
    def interface(self):
        if self._interface is None:
            raise NoInterfaceSet()
        return self._plugins[self._interface][2]

    @interface.setter
    def interface(self, id_):
        if id not in self._threads['query']:
            raise NoInterface(id_)
        self._interface = id_

    def find_plugins(self, dirs):
        for attr in Plugins.__dict__:
            if Plugins.__dict__[attr] is list:
                Plugins.__dict__[attr] = []
        for dir_ in dirs:
            for item in os.listdir(dir_):
                name, ext = os.path.splitext(item)
                if ext != '.py' and not os.path.isdir(item):
                    continue
                file_, path, descr = imp.find_module(name, [dir_])
                if file_:
                    imp.load_module(name, file_, path, descr)

    def load_plugin(self, name, desc, plugin, args=()):
        if type(args) is not list or type(args) is not tuple:
            args = (args)
        plugin_id = self._plugin_id
        self._plugin_id += 1
        try:
            plugin = self._available[plugin]
        except KeyError:
            raise MissingPlugin(plugin)
        try:
            instance = plugin(*(args,))
        except:
            # raise PluginInitError(plugin)
            raise
        self._plugins[plugin_id] = (name, desc, instance)
        if 'input' in plugin.type:
            self._threads['input'][plugin_id] = {}
        if 'storage' in plugin.type:
            self._threads['storage'][plugin_id] = {}
        if 'query' in plugin.type:
            self._threads['query'][plugin_id] = {}
            if self._interface is None:
                self._interface = plugin_id
        return plugin_id

    def unload_plugin(self, id_):
        if id_ in self._threads['input']:
            map(self.disconnect_plugins, self._threads['input'][id_].keys())
            del self._threads['input'][id_]
        if id_ in self._threads['storage']:
            map(self.disconnect_plugins, self._threads['storage'][id_].keys())
            del self._threads['storage'][id_]
        if id_ in self._threads['query']:
            del self._threads['query'][id_]
            if self._interface == id_:
                self._interface = None
        if id_ in self._plugins:
            self._plugins[id_][2].unload()
            del self._plugins[id_]
        else:
            raise Exception("plugin does not exist")

    def connect_plugins(self, input_, storage):
        callback = self._plugins[storage][2].enqueue
        tid = self._plugins[input_][2].start(callback)[0]
        self._threads['input'][input_][tid] = storage
        self._threads['storage'][storage][tid] = input_
        self._connections[tid] = (input_, storage)
        return tid

    def disconnect_plugins(self, tid):
        input_, storage = self._connections[tid]
        self._plugins[input_][2].stop_thread(tid)
        del self._connections[tid]
        del self._threads['storage'][storage][tid]
        del self._threads['input'][input_][tid]
