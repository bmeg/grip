import unittest

from gripql.connection import Connection
from gripql.util import BaseConnection


class TestUrlBuilding(unittest.TestCase):
    mock_url = "http://fakehost:8000"

    def test_url_building(self):
        b = BaseConnection(self.mock_url)
        self.assertEqual(b.base_url, self.mock_url)

        c = Connection(self.mock_url)
        self.assertEqual(c.base_url, self.mock_url)
        self.assertEqual(c.url, self.mock_url + "/v1/graph")

        g = c.graph("test")
        self.assertEqual(g.base_url, self.mock_url)
        self.assertEqual(g.url, self.mock_url + "/v1/graph/test")

        ba = g.bulkAdd()
        self.assertEqual(ba.base_url, self.mock_url)
        self.assertEqual(ba.url, self.mock_url + "/v1/graph")

        q = g.query()
        self.assertEqual(q.base_url, self.mock_url)
        self.assertEqual(q.url, self.mock_url + "/v1/graph/test/query")
