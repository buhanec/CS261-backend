import sys
import cdecimal
from system2 import TheSystem
import time
import traceback


if __name__ == '__main__':
    # performance stuff for older python < 3
    sys.modules["decimal"] = cdecimal

    system = TheSystem()

    trades = ("cs261.dcs.warwick.ac.uk", 80)
    in_ = system.load_plugin(None, None, "Network Capture", (trades,))
    out = system.load_plugin(None, None, "Printing Storage")
    tid = system.connect_plugins(in_, out)
    time.sleep(3)
    system.disconnect_plugins(tid)
    system.unload_plugin(in_)
    system.unload_plugin(out)
    time.sleep(1)

    print >> sys.stderr, "\n*** STACKTRACE - START ***"
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
