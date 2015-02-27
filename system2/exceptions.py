""" Module defines exceptions used by system2 """


class System2Exception(Exception):
    """ Base exception raised for error conditions """


class NoInterfaceSetException(System2Exception):
    """ Raised when the interface is being accessed but no interface is
    currently selected
    """

    def __init__(self):
        super(NoInterfaceSetException).__init__()


class NoInterfaceException(System2Exception):
    """ Raised when trying to set an interface that does not exist """

    def __init__(self, id_):
        """ @param id_: the id trying to be set """
        super(NoInterfaceException).__init__(id_)
        self.id_ = id_
