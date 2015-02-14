import imp
import os


class TheSystem(object):
    """ The core class """

    def __init__(self):
        print('[TheSystem] init')
        self._inputs = {}
        self._storages = {}

    def find_plugins(self, dirs):
        for dir in dirs:
            for filename in os.listdir(dir):
                name, ext = os.path.splitext(filename)
                if ext == '.py':
                    file, path, descr = imp.find_module(name, [dir])
                    if file:
                        imp.load_module(name, file, path, descr)

    def load_plugin(self, name, desc, plugin):
        pass

    def unload_plugin(self, id):
        pass

    def connect_input(self, input, storage):
        pass

    def disconnect_input(self, input, storage):
        pass

    def connect_query(self, query, storage):
        pass

    def disconnect_query(self, query, storage):
        pass
