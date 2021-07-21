
import os
import sys
import string
import random
import yaml
import unittest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRIPQL = os.path.join(os.path.dirname(BASE), "gripql", "python")
sys.path.insert(0,GRIPQL)

import gripql

SERVER = None

class TestTableList(unittest.TestCase):
    def test_table_list(self):
        conn = gripql.Connection(SERVER)
        found = False
        for r in conn.listTables():
            if r['source'] == 'tableServer':
                found = True
        self.assertTrue(found)

    def test_post_mapping(self):
        with open(os.path.join(BASE, "test-graph/swapi.yaml")) as handle:
            mappingGraph = yaml.load(handle.read())
        conn = gripql.Connection(SERVER)
        graphName = "posted_tabledata_%s" % (''.join(random.choices(string.ascii_uppercase + string.digits, k=4)))
        conn.postMapping(graphName, mappingGraph['vertices'], mappingGraph['edges'])
        graphs = list(conn.listGraphs())
        self.assertTrue( graphName in graphs )
        self.assertTrue( graphName + "__mapping__" in graphs )

if __name__ == '__main__':
    SERVER = sys.argv.pop(-1)
    print(sys.argv)
    unittest.main()
