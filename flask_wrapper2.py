import sys
import cdecimal
import decimal
import datetime
from system2 import TheSystem
import traceback
from flask import Flask, request, make_response
import signal
import json
import subprocess
sys.modules["decimal"] = cdecimal

app = Flask(__name__)
system = TheSystem()
sql_id = 2


def jstr(l):
    for k, v in enumerate(l):
        if isinstance(v, decimal.Decimal) or isinstance(v, cdecimal.Decimal):
            l[k] = str(v)
        elif isinstance(v, datetime.datetime):
            l[k] = v.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+'Z'
    return l


def api_repr(value):
    if type(value) is list or type(value) is tuple:
        l = [jstr(row) for row in value]
    else:
        l = str(value)
    content = json.dumps(l)
    response = make_response(content)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Content-type'] = 'application/json'
    return response


@app.route("/plugins/", methods=['GET'])
def f1():
    return api_repr(system.plugins)


@app.route("/plugins/loaded/", methods=['GET'])
def f2():
    return api_repr(system.loaded_plugins)


@app.route("/plugins/loaded/input/", methods=['GET'])
def f3():
    return api_repr(system.loaded_input_plugins)


@app.route("/plugins/loaded/storage/", methods=['GET'])
def f4():
    return api_repr(system.loaded_storage_plugins)


@app.route("/plugins/loaded/query/", methods=['GET'])
def f5():
    return api_repr(system.loaded_query_plugins)


@app.route("/plugins/loaded/<string:plugin>", methods=['PUT'])
def f6(plugin):
    name = request.data.get('name', None)
    desc = request.data.get('desc', None)
    args = request.data.get('args', ())
    plugin_id = system.load_plugin(name, desc, plugin, args)
    # plugin_id = system.load_plugin(name, desc, plugin, 'sqlite://')
    return api_repr(plugin_id)


@app.route("/plugins/loaded/<int:id_>", methods=['DELETE'])
def f7(id_):
    try:
        system.unload_plugin(id_)
        return api_repr(True)
    except:
        return api_repr(False)


@app.route("/connections", methods=['GET'])
def f8():
    return api_repr(system.connections)


@app.route("/connections/<int:input_>/<int:storage>", methods=['PUT'])
def f9(input_, storage):
    try:
        return api_repr(system.connect_plugins(input_, storage))
    except:
        return api_repr(False)


@app.route("/connections/<int:tid>", methods=['DELETE'])
def f10(tid):
    try:
        system.disconnect_plugins(tid)
        return api_repr(True)
    except:
        return api_repr(False)


@app.route("/interface/", methods=['GET'])
def f11():
    return api_repr(system.interface_id)


@app.route("/interface/<int:id_>", methods=['PUT'])
def f12(id_):
    try:
        system.interface = id_
        return api_repr(True)
    except:
        return api_repr(False)


@app.route("/data/trades/<int:number>", methods=['GET'])
def f13(number):
    try:
        return api_repr(system.interface.trades(None, number))
    except:
        raise
        return api_repr(False)


@app.route("/data/trade/<int:number>", methods=['GET'])
def f13_(number):
    try:
        return api_repr(system.interface.trade(number))
    except:
        raise
        return api_repr(False)


@app.route("/data/comms/<int:number>", methods=['GET'])
def f15(number):
    try:
        return api_repr(system.interface.comms(None, number))
    except:
        return api_repr(False)


@app.route("/data/alerts/<int:number>", methods=['GET'])
def f17(number):
    try:
        return api_repr(system.interface.alerts(None, number))
    except:
        raise
        return api_repr(False)


@app.route("/data/stock/<string:symbol>", methods=['GET'])
def f19(symbol):
    try:
        return api_repr(system.interface.stock(symbol))
    except:
        return api_repr(False)


@app.route("/data/trader/<string:email>", methods=['GET'])
def f20(email):
    try:
        return api_repr(system.interface.trader(email))
    except:
        return api_repr(False)


@app.route("/data/alert/<int:alertid>", methods=['GET'])
def f21(alertid):
    try:
        return api_repr(system.interface.alert(alertid))
    except:
        return api_repr(False)


@app.route("/file/<string:name>", methods=['GET'])
def f22(name):
    global sql_id
    try:
        path = "/srv/www/files/"+name
        pluginid = system.load_plugin('FileReader', None, 'FileReader', path)
        system.connect_plugins(pluginid, sql_id)
        return api_repr(pluginid)
    except:
        return api_repr(False)


def init():
    global sql_id
    # Params for data collection
    trades = ('cs261.dcs.warwick.ac.uk', 80)
    comms = ('cs261.dcs.warwick.ac.uk', 1720)
    db = 'mysql+mysqldb://CS261:password@127.0.0.1/CS261'
    # Load plugins
    trades_id = system.load_plugin('Trades', None, 'NetCap', trades)
    comms_id = system.load_plugin('Comms', None, 'NetCap', comms)
    # printer_id = system.load_plugin('Printer', None, 'Printer', None)
    sql_id = system.load_plugin('SQL', None, 'SQLStore', db)
    # Connect plugins
    system.connect_plugins(trades_id, sql_id)
    # system.connect_plugins(trades_id, printer_id)
    system.connect_plugins(comms_id, sql_id)
    # system.connect_plugins(comms_id, printer_id)


def cleanup():
    system.unload_all()


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def handler(signal, frame):
    print ""
    cleanup()
    shutdown_server()

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

    sys.exit(0)


if __name__ == '__main__':
    # Start system
    init()

    # handler
    signal.signal(signal.SIGINT, handler)

    # flask
    app.run(debug=False, host='0.0.0.0')

    while True:
        signal.pause()
