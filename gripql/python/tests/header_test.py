import unittest

from gripql.util import BaseConnection


class TestRequestHeaderFormat(unittest.TestCase):
    mock_url = "http://fakehost:8000"

    def test_connection(self):
        h = BaseConnection(self.mock_url)._request_header()
        self.assertEqual(h, {'Content-type': 'application/json'})

        h = BaseConnection(self.mock_url, user="test", password="password")._request_header()
        self.assertEqual(h, {'Content-type': 'application/json', 'Authorization': 'Basic dGVzdDpwYXNzd29yZA=='})

        h = BaseConnection(self.mock_url, token="test")._request_header()
        self.assertEqual(h, {'Content-type': 'application/json', 'Authorization': 'Bearer test'})
