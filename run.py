from system2 import TheSystem
from system2.plugins import Plugins
import time

if __name__ == '__main__':
    # this just servers for testing right now, actual system is todo
    # we can still implement plugin functionality & define interfaces
    system = TheSystem()
    system.find_plugins(["plugins"])

    print "Plugins:", [p.name for p in Plugins._plugins]
    print "Input:", [p.name for p in Plugins._input]
    print "Storage:", [p.name for p in Plugins._storage]

    tcap = Plugins._input[1](("cs261.dcs.warwick.ac.uk", 80))
    stor = Plugins._storage[1]()
    tid = tcap.start(stor)
    tid2 = tcap.start(stor)
    time.sleep(1)
    tcap.stop(tid)
    tcap.stop(tid2)
