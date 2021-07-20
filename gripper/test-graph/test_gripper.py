
import os
import sys
import yaml
import unittest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRIPQL = os.path.join(os.path.dirname(BASE), "gripql", "python")
sys.path.insert(0,GRIPQL)

import gripql

SERVER = None

class TestTableList(unittest.TestCase):
    def test_table_list(self):
        print("Do something here: %s " % (SERVER) )
        conn = gripql.Connection(SERVER)
        for r in conn.listTables():
            print(r)

    def test_post_mapping(self):
        with open(os.path.join(BASE, "test-graph/swapi.yaml")) as handle:
            mappingGraph = yaml.load(handle.read())
        conn = gripql.Connection(SERVER)
        conn.postMapping("posted_tabledata", mappingGraph['vertices'], mappingGraph['edges'])
        for l in conn.listGraphs():
            print(l)

if __name__ == '__main__':
    SERVER = sys.argv.pop(-1)
    print(sys.argv)
    unittest.main()
