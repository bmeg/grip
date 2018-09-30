import unittest

from gripql.query import Query


class TestQueryFormat(unittest.TestCase):

    def test_wrapping_none(self):
        q = Query("localhost", "test")
        self.assertEqual(q.V().in_().to_json(), q.V().in_(None).to_json())
        self.assertEqual(q.V().out().to_json(), q.V().out(None).to_json())
        self.assertEqual(q.V().both().to_json(), q.V().both(None).to_json())
        with self.assertRaises(TypeError):
            q.V().in_(["foo", None]).to_json()
        with self.assertRaises(TypeError):
            q.V().in_(["foo", 1]).to_json()
        with self.assertRaises(TypeError):
            q.V().in_(1).to_json()
