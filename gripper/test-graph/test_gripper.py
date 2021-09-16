
import os
import sys
import string
import random
import yaml
import unittest
import time
import subprocess

GRIPDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRIPQL = os.path.join(os.path.dirname(BASE), "gripql", "python")
sys.path.insert(0,GRIPQL)

import gripql

class TestTableList(unittest.TestCase):
    def test_plugin_start(self):
        subp = subprocess.Popen([os.path.join(GRIPDIR, "grip"), "server", "-p", os.path.join(BASE, "test-graph")])

        print("Waiting for server to start")
        time.sleep(2)
        SERVER = "http://localhost:8201"
        try:
            conn = gripql.Connection(SERVER)
            res = conn.startPlugin("tableServer", "table", {"path": os.path.join(BASE, "test-graph/swapi/table.map") })
            print(res)
            found = False
            count = 0
            while count < 5 and not found:
                time.sleep(1)
                print("Listing Tables")
                for r in conn.listTables():
                    print("table", r)
                    if r['source'] == 'tableServer':
                        found = True
                count += 1
            self.assertTrue(found)

            with open(os.path.join(BASE, "test-graph/swapi.yaml")) as handle:
                mappingGraph = yaml.load(handle.read())
            conn = gripql.Connection(SERVER)
            graphName = "posted_tabledata_%s" % (''.join(random.choices(string.ascii_uppercase + string.digits, k=4)))
            conn.postMapping(graphName, mappingGraph['vertices'], mappingGraph['edges'])
            graphs = list(conn.listGraphs())
            self.assertTrue( graphName in graphs )
            self.assertTrue( graphName + "__mapping__" in graphs )

            G = conn.graph(graphName)
            count = 0
            for v in G.query().V():
                count += 1
            self.assertTrue( count == 39 )
        except Exception as e:
            subp.kill()
            raise e
        subp.kill()

if __name__ == '__main__':
    unittest.main()
