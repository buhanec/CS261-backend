""" Module defines exceptions used by system2 """


class System2Exception(Exception):
    """ Base exception raised for error conditions """


class NoInterfaceSet(System2Exception):
    """ Raised when the interface is being accessed but no interface is
    currently selected
    """


class NoInterface(System2Exception):
    """ Raised when trying to set an interface that does not exist """

    def __init__(self, id_):
        """ @param id_: the id trying to be set """
        super(NoInterface).__init__(id_)
        self.id_ = id_


class MissingPlugin(System2Exception):
    """ Raised when trying to manipulate a non-existing plugin """

    def __init__(self, value):
        """ @param value: id or name of the plugin """
        super(MissingPlugin).__init__(value)
        self.value = value


class PluginInitError(System2Exception):
    """ Raised when a plugin cannot initalise correctly """

    def __init__(self, value):
        """ @param value: id or name of the plugin """
        super(PluginInitError).__init__(value)
        self.value = value
