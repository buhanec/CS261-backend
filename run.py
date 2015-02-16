import sys
import cdecimal
from system2 import TheSystem
import time
import traceback


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
    network = system.input_plugins[0]
    printer = system.storage_plugins[1]

    in_ = system.load_plugin(None, None, network, (trades,))
    out = system.load_plugin(None, None, printer)
    tid = system.connect_plugins(in_, out)
    time.sleep(1)
    system.disconnect_plugins(tid)
    system.unload_plugin(in_)
    system.unload_plugin(out)
    time.sleep(1)

    print >> sys.stderr, "\n*** STACKTRACE - START ***\n"
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename,
                                                        lineno, name))
            if line:
                code.append("  %s" % (line.strip()))

    for line in code:
        print >> sys.stderr, line
    print >> sys.stderr, "\n*** STACKTRACE - END ***\n"
