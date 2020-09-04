from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import random
import string
import sys
import json
import traceback

from glob import glob


BASE = os.path.dirname(os.path.abspath(__file__))
TESTS = os.path.join(BASE, "tests")
GRIPQL = os.path.join(os.path.dirname(BASE), "gripql", "python")
sys.path.append(GRIPQL)
import gripql  # noqa: E402


try:
    from importlib.machinery import SourceFileLoader

    def load_test_mod(name):
        return SourceFileLoader('test.%s' % name, os.path.join(TESTS, name + ".py")).load_module()
except ImportError:
    # probably running older python without newer importlib
    import imp

    def load_test_mod(name):
        return imp.load_source('test.%s' % name, os.path.join(TESTS, name + ".py"))


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size)).lower()


class SkipTest(Exception):
    pass


class Manager:
    def __init__(self, conn, readOnly=False):
        self._conn = conn
        self.readOnly = readOnly
        self.curGraph = ""
        self.curName = ""

    def newGraph(self):
        if self.readOnly is None:
            self.curGraph = "test_graph_" + id_generator()
            self._conn.addGraph(self.curGraph)
        else:
            self.curGraph = args.readOnly

    def setGraph(self, name):
        if self.readOnly is not None:
            return self._conn.graph(self.readOnly)

        if self.curName == name:
            return self._conn.graph(self.curGraph)

        if self.curGraph != "":
            self.clean()

        self.curGraph = "test_graph_" + id_generator()
        self._conn.addGraph(self.curGraph)

        G = self._conn.graph(self.curGraph)

        with open(os.path.join(BASE, "graphs", "%s.vertices" % (name))) as handle:
            for line in handle:
                data = json.loads(line)
                G.addVertex(data["gid"], data["label"], data.get("data", {}))

        with open(os.path.join(BASE, "graphs", "%s.edges" % (name))) as handle:
            for line in handle:
                data = json.loads(line)
                G.addEdge(src=data["from"], dst=data["to"],
                          gid=data.get("gid", None), label=data["label"],
                          data=data.get("data", {}))
        self.curName = name
        return G

    def clean(self):
        if self.readOnly is None:
            self._conn.deleteGraph(self.curGraph)

    def writeTest(self):
        if self.readOnly is not None:
            raise SkipTest
        self.clean()
        self.curName = ""
        self.curGraph = "test_graph_" + id_generator()
        self._conn.addGraph(self.curGraph)
        G = self._conn.graph(self.curGraph)
        return G


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "server",
        type=str,
        help="GRIP server url"
    )
    parser.add_argument(
        "tests",
        type=str,
        nargs='*',
        help="conformance test suite(s) to run"
    )
    parser.add_argument(
        "--exclude",
        "-e",
        type=str,
        nargs="+",
        default=[],
        help="Exclude test suite(s)"
    )
    parser.add_argument(
        "--methods",
        "-m",
        type=str,
        nargs="+",
        default=[],
        help="Unit Test Methods"
    )
    parser.add_argument(
        "--readOnly",
        "-r",
        default=None
    )
    args = parser.parse_args()
    server = args.server
    if len(args.tests) > 0:
        tests = ["ot_" + t for t in args.tests]
    else:
        tests = [os.path.basename(a)[:-3] for a in glob(os.path.join(TESTS, "ot_*.py"))]
    # filter out excluded tests
    tests = [t for t in tests if t[3:] not in args.exclude]

    conn = gripql.Connection(server)

    correct = 0
    total = 0
    manager = Manager(conn, args.readOnly)
    for name in tests:
        mod = load_test_mod(name)
        for f in dir(mod):
            if f.startswith("test_"):
                func = getattr(mod, f)
                if callable(func):
                    if len(args.methods) == 0 or f[5:] in args.methods:
                        try:
                            print("Running: %s %s " % (name, f[5:]))
                            try:
                                e = func(manager)
                            except SkipTest:
                                continue
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
    manager.clean()

    print("Passed %s out of %s" % (correct, total))
    if correct != total:
        sys.exit(1)
