#!/usr/bin/env python

import os
import sys
import imp
import aql
from glob import glob
import traceback

BASE = os.path.dirname(__file__)
TESTS = os.path.join(BASE, "tests")

def clear_db(O):
    O.query().V().drop().execute()
    if int(O.query().V().count().first()['int_value']) != 0:
        print "Unable to clear database"
        sys.exit()
    if int(O.query().E().count().first()['int_value']) != 0:
        print "Unable to clear database"
        sys.exit()

if __name__ == "__main__":
    server = sys.argv[1]

    O = aql.Connection(server)
    if int(O.query().V().count().first()['int_value']) != 0:
        print "Need to start with empty DB"
        sys.exit()

    correct = 0
    total = 0
    for a in glob(os.path.join(TESTS, "ot_*.py")):
        name = os.path.basename(a)[:-3]
        mod = imp.load_source('test.%s' % name, a)
        for f in dir(mod):
            if f.startswith("test_"):
                func = getattr(mod,f)
                if callable(func):
                    try:
                        e = func(O)
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
                    clear_db(O)

    print "Passed %s out of %s" % (correct, total)
