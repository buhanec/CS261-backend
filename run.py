import sys
import cdecimal
from system2 import TheSystem
import time

if __name__ == '__main__':
    # performance stuff for older python < 3
    sys.modules["decimal"] = cdecimal

    # this just servers for testing right now, actual system is todo
    # we can still implement plugin functionality & define interfaces
    system = TheSystem()
    system.find_plugins(["plugins"])

    print "Plugins:", [p.name for p in system.plugins]
    print "Input:", [p.name for p in system.input_plugins]
    print "Storage:", [p.name for p in system.storage_plugins]

    trades = ("cs261.dcs.warwick.ac.uk", 80)
    network = system.input_plugins[1]
    printer = system.storage_plugins[1]

    in_ = system.load_plugin(None, None, network, (trades,))
    out = system.load_plugin(None, None, printer)
    tid = system.connect_plugins(in_, out)
    time.sleep(3)
    system.disconnect_plugins(tid)
