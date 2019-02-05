import unittest

from gripql.util import BaseConnection


class TestBaseConnectionInit(unittest.TestCase):

    def test_proccess_url(self):

        b = BaseConnection("fakehost:8000")
        self.assertEqual(b.url, "http://fakehost:8000")

        b = BaseConnection("https://fakehost:8000")
        self.assertEqual(b.url, "https://fakehost:8000")

        b = BaseConnection("http://fakehost:8000")
        self.assertEqual(b.url, "http://fakehost:8000")

        b = BaseConnection("http://fakehost")
        self.assertEqual(b.url, "http://fakehost")

        b = BaseConnection("http://fakehost:8000/v1/graph")
        self.assertEqual(b.url, "http://fakehost:8000")

        b = BaseConnection("http://fakehost/v1/graph")
        self.assertEqual(b.url, "http://fakehost")

        b = BaseConnection("http://fakehost:8000/v1/graph/test_graph")
        self.assertEqual(b.url, "http://fakehost:8000")
