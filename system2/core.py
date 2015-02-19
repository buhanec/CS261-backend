import imp
import logger
import os
from .plugins import Plugins


class TheSystem(object):
    """ The core class """

    def __init__(self):
        logger.logger.info('[TheSystem] init')
        # probably need to change this at some point
        self._plugins = {}
        self._input_threads = {}
        self._storage_threads = {}
        self._query_threads = {}
        self._connections = {}
        self._plugin_id = 0

    # make read-only class for all these properties
    # also cache keys for performance or just recreate DS
    @property
    def plugins(self):
        return Plugins._plugins

    @property
    def input_plugins(self):
        return Plugins._input

    @property
    def storage_plugins(self):
        return Plugins._storage

    @property
    def query_plugins(self):
        return Plugins._query_plugins

    @property
    def loaded_plugins(self):
        return self._plugins().keys()

    @property
    def loaded_input_plugins(self):
        return self._input_threads.keys()

    @property
    def loaded_storage_plugins(self):
        return self._input_threads.keys()

    @property
    def loaded_query_plugins(self):
        return self._query_threads.keys()

    def find_plugins(self, dirs):
        for dir in dirs:
            for filename in os.listdir(dir):
                name, ext = os.path.splitext(filename)
                if ext == '.py':
                    file, path, descr = imp.find_module(name, [dir])
                    if file:
                        imp.load_module(name, file, path, descr)

    def load_plugin(self, name, desc, plugin, args=()):
        plugin_id = self._plugin_id
        self._plugin_id += 1
        instance = plugin(*args)
        self._plugins[plugin_id] = (name, desc, instance)
        if 'input' in plugin.type:
            self._input_threads[plugin_id] = {}
        if 'storage' in plugin.type:
            self._storage_threads[plugin_id] = {}
        if 'query' in plugin.type:
            self._query_threads[plugin_id] = {}
        return plugin_id

    def unload_plugin(self, id_):
        if id_ in self._input_threads:
            map(self.disconnect_plugins, self._input_threads[id_].keys())
            del self._input_threads[id_]
        if id_ in self._storage_threads:
            map(self.disconnect_plugins, self._storage_threads[id_].keys())
            del self._storage_threads[id_]
        if id_ in self._query_threads:
            del self._query_threads[id_]
        if id_ in self._plugins:
            self._plugins[id_][2].unload()
            del self._plugins[id_]
        else:
            raise Exception("plugin does not exist")

    def connect_plugins(self, input_, storage):
        tid = self._plugins[input_][2].start(self._plugins[storage][2].store)[0]
        self._input_threads[input_][tid] = storage
        self._storage_threads[storage][tid] = input_
        self._connections[tid] = (input_, storage)
        return tid

    def disconnect_plugins(self, tid):
        input_, storage = self._connections[tid]
        self._plugins[input_][2].stop_thread(tid)
        del self._connections[tid]
        del self._storage_threads[storage][tid]
        del self._input_threads[input_][tid]

