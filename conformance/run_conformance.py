from __future__ import absolute_import, print_function, unicode_literals

import os
import sys
import imp
from glob import glob
import traceback

BASE = os.path.dirname(os.path.abspath(__file__))
TESTS = os.path.join(BASE, "tests")
AQL = os.path.join(os.path.dirname(BASE), "aql", "python")
GRAPH = "test_graph"
sys.path.append(AQL)
import aql


if __name__ == "__main__":
    server = sys.argv[1]
    if len(sys.argv) > 2:
        tests = sys.argv[2:]
    else:
        tests = []

    conn = aql.Connection(server)
    if GRAPH in conn.listGraphs():
        if int(conn.graph(GRAPH).query().V().count().first()['data']) != 0:
            print("Need to start with empty DB: %s" % (GRAPH))
            sys.exit()

    correct = 0
    total = 0
    for a in glob(os.path.join(TESTS, "ot_*.py")):
        name = os.path.basename(a)[:-3]
        if len(tests) == 0 or name[3:] in tests:
            mod = imp.load_source('test.%s' % name, a)
            for f in dir(mod):
                if f.startswith("test_"):
                    func = getattr(mod, f)
                    if callable(func):
                        try:
                            print("Running: %s %s " % (name, f[5:]))
                            conn.addGraph(GRAPH)
                            e = func(conn.graph(GRAPH))
                            if len(e) == 0:
                                correct += 1
                                print("Passed: %s %s " % (name, f[5:]))
                            else:
                                print("Failed: %s %s " % (name, f[5:]))
                                for i in e:
                                    print("\t- %s" % (i))
                        except Exception as e:
                            print("Crashed: %s %s %s" % (name, f[5:], e))
                            traceback.print_exc()
                        total += 1
                        conn.deleteGraph(GRAPH)

    print("Passed %s out of %s" % (correct, total))
    if correct != total:
        sys.exit(1)
