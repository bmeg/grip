#!/usr/bin/env python

import os
import sys
import imp
from glob import glob
import traceback

BASE = os.path.dirname(os.path.abspath(__file__))
TESTS = os.path.join(BASE, "tests")

GRAPH = "test_graph"

sys.path.append( os.path.dirname(BASE) )

import aql


def clear_db(conn):
    conn.delete(GRAPH)
    conn.new(GRAPH)
    O = conn.graph(GRAPH)
    if int(O.query().V().count().first()['int_value']) != 0:
        print "Unable to clear database"
        sys.exit()
    if int(O.query().E().count().first()['int_value']) != 0:
        print "Unable to clear database"
        sys.exit()

if __name__ == "__main__":
    server = sys.argv[1]
    tests = sys.argv[2:]

    conn = aql.Connection(server)
    if int(conn.graph(GRAPH).query().V().count().first()['int_value']) != 0:
        print "Need to start with empty DB"
        sys.exit()

    correct = 0
    total = 0
    for a in glob(os.path.join(TESTS, "ot_*.py")):
        name = os.path.basename(a)[:-3]
        if len(tests) == 0 or name[3:] in tests:
            mod = imp.load_source('test.%s' % name, a)
            for f in dir(mod):
                if f.startswith("test_"):
                    func = getattr(mod,f)
                    if callable(func):
                        try:
                            e = func(conn.graph(GRAPH))
                            if len(e) == 0:
                                correct += 1
                                print "Passed: %s %s " % (name, f[5:])
                            else:
                                print "Failed: %s %s " % (name, f[5:])
                                for i in e:
                                    print "\t- %s" % (i)
                        except Exception, e:
                            print "Crashed: %s %s %s" % (name, f[5:], e)
                            traceback.print_exc()
                        total += 1
                        clear_db(conn)

    print "Passed %s out of %s" % (correct, total)
    if correct != total:
        sys.exit(1)
