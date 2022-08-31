from __future__ import absolute_import, print_function, unicode_literals

import argparse
import csv
import json
import random
import string

import os
import sys
import traceback
import uuid
from collections import namedtuple
from glob import glob
import yaml

# import grip from source
from typing import List

BASE = os.path.dirname(os.path.abspath(__file__))
TESTS = os.path.join(BASE, "tests")
GRIPQL = os.path.join(os.path.dirname(BASE), "gripql", "python")
sys.path.insert(0,GRIPQL)
import gripql  # noqa: E402


# test loader
try:
    from importlib.machinery import SourceFileLoader

    def load_test_mod(name):
        return SourceFileLoader('test.%s' % name, os.path.join(TESTS, name + ".py")).load_module()
except ImportError:
    # probably running older python without newer importlib
    import imp

    def load_test_mod(name):
        return imp.load_source('test.%s' % name, os.path.join(TESTS, name + ".py"))


class SkipTest(Exception):
    """A target test can raise this to ignore test."""
    pass


def create_arg_parser():
    """Common arguments."""

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
    parser.add_argument(
        "--user",
        "-u",
        default=None
    )
    parser.add_argument(
        "--password",
        "-p",
        default=None
    )
    parser.add_argument(
        "--grip_config_file_path",
        default=None
    )

    args = parser.parse_args()
    return args


Policy = namedtuple("Policy", "sub obj act")
Account = namedtuple("Account", "access user password policies is_admin")


class Manager:
    """Common test methods."""

    def __init__(self, conn, readOnly=False, server=None, grip_config_file_path=None):
        self.readOnly = readOnly
        self.curGraph = ""
        self.curName = ""
        self.grip_config = self.parse_grip_config(grip_config_file_path)
        self.access = None
        self.policies = None
        self.accounts = []
        self.all_graph_names = []
        if self.grip_config:
            self.access = self.grip_config['Server']['Access']
            self.policies = self.load_policies(self.access['Policy'])
            self.all_graph_names = list(set(policy.obj for policy in self.policies if policy.obj != '*')) + ['dummy']
            self.accounts = []
            for account in self.grip_config['Server']['Accounts']['Auth']['Basic']:
                account_policies = [policy for policy in self.policies if policy.sub == account['User']]
                is_admin = all([policy.obj == '*' and policy.act == '*' for policy in account_policies])
                self.accounts.append(Account(*['Basic', account['User'], account['Password'], account_policies, is_admin]))
        self.graphs = None
        self._conn = None
        self.user = None
        self.server = server
        self.set_connection(conn)

    def set_connection(self, conn):
        """Set conn and user property"""
        self._conn = conn
        if self._conn:
            self.user = self._conn.user
        else:
            self.user = None

    @staticmethod
    def parse_grip_config(grip_config_file_path):
        """Parse grip config."""
        if not grip_config_file_path:
            return None
        with open(grip_config_file_path) as file:
            grip_config = yaml.load(file, Loader=yaml.FullLoader)
            return grip_config

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
        if self.readOnly is None and self.curGraph != "":
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

    def run_tests(self, tests, args):
        correct = 0
        total = 0
        connections = []
        if self._conn:
            connections.append(self._conn)
        for account in self.accounts:
            connections.append(create_connection(self.server, account.user, account.password))
        for connection in connections:
            self.set_connection(connection)
            print(f"Running tests for {connection.user}")
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
                                        e = func(self)
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
            self.clean()
        return correct, total

    @staticmethod
    def load_policies(file_path) -> List[Policy]:
        """Load csv file of policies, return list of namedtuple [(sub, obj, act)]."""
        if not file_path:
            return None
        policies_ = []
        with open(file_path) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                # clean up spaces
                row = [col.strip() for col in row]
                assert len(row) == 4, f"Unexpected number of columns {row}"
                assert row[0] == 'p', f"Unknown row type {row}"
                policies_.append(Policy(*row[-3:]))
        return policies_

    def create_graphs_from_policies(self):
        """Get obj aka graph names, except wild card then create graphs."""
        if not self.policies:
            return None
        graph_names = set([policy.obj for policy in self.policies if policy.obj != '*'])
        assert len(graph_names) > 0, "Could not load graph_names"
        # create graphs, including a dummy that no-one other than admin should be able to read
        for graph_name in list(graph_names) + ['dummy']:
            self._conn.deleteGraph(graph_name)
            self._conn.addGraph(graph_name)
            G = self._conn.graph(graph_name)
            bulk = G.bulkAdd()
            bulk.addVertex("Foo:1", "Foo", {"bar": "foo-bar"})
            err = bulk.execute()
            assert err['insertCount'] == 1 and err['errorCount'] == 0, f"Did not insert 1 row {err}"
            results = [v for v in G.query().V().hasLabel("Foo").count()]
            assert results[0]['count'] == 1, f"Could not query Foo vertex. {results}"
        return graph_names

    def test_query(self, graph_name):
        """Ensure the current user can query graph_name."""
        G = self._conn.graph(graph_name)
        results = [v for v in G.query().V().hasLabel("Foo").count().execute()]
        assert results[0]['count'] > 0, f"test_read {results}"

    def test_read(self, graph_name):
        """Ensure the current user can query graph_name."""
        G = self._conn.graph(graph_name)
        results = [G.getVertex("Foo:1")]
        assert len(results) == 1, f"test_read {results}"

    def test_write(self, graph_name):
        """Ensure the current user can write to graph_name."""
        G = self._conn.graph(graph_name)
        bulk = G.bulkAdd()
        id_ = uuid.uuid1()
        bulk.addVertex(f"Foo:{id_}", "Foo", {"bar": "foo-bar"})
        err = bulk.execute()
        assert err['insertCount'] == 1 and err['errorCount'] == 0, f"Did not insert 1 row {err}"

    def current_user_policies(self):
        """Get all policies for current connection user."""
        return [policy for policy in self.policies if policy.sub == self.user]

    def current_user_account(self):
        """Get account for current connection user."""
        return next(iter(account for account in self.accounts if account.user == self.user), None)


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """Random 6 alpha numeric string."""
    return ''.join(random.choice(chars) for _ in range(size)).lower()


def filter_tests(args, prefix="ot_"):
    """Filter test modules."""
    if len(args.tests) > 0:
        tests_ = [prefix + t for t in args.tests]
    else:
        tests_ = [os.path.basename(a)[:-3] for a in glob(os.path.join(TESTS, f"{prefix}*.py"))]
    # filter out excluded tests
    tests_ = [t for t in tests_ if t[3:] not in args.exclude]
    return tests_


def create_connection(server, user, password):
    """Setup connection based on passed credentials."""
    return gripql.Connection(server, user=user, password=password)
