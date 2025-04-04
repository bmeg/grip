
import pygrip
import unittest

class TestPyGRIP(unittest.TestCase):

    def test_query(self):

        w = pygrip.NewMemServer()

        w.addVertex("1", "Person", {"age":30, "eyes":"brown"})
        w.addVertex("2", "Person", {"age":40, "eyes":"blue"})
        w.addEdge("1", "2", "knows")

        count = 0
        for row in w.V().hasLabel("Person"):
            count += 1        
        self.assertEqual(count, 2)

        count = 0
        for row in w.V().out("knows"):
            count += 1
        self.assertEqual(count, 1)
        
        for row in w.V().count():
            self.assertEqual(row["count"], 2)

if __name__ == '__main__':
    unittest.main()
