import json
import os
import tempfile
import unittest

from gripql import Connection
from gripql.util import BaseConnection


def headersOverlap(actual, expected):
    for k, v in expected.items():
        assert k in actual
        assert actual[k] == v


class TestRequestHeaderFormat(unittest.TestCase):
    mock_url = "http://fakehost:8000"

    def test_connection(self):
        b = BaseConnection(self.mock_url)
        headersOverlap(b.session.headers, {'Content-type': 'application/json'})

        b = BaseConnection(self.mock_url, user="test", password="password")
        headersOverlap(b.session.headers, {'Content-type': 'application/json', 'Authorization': 'Basic dGVzdDpwYXNzd29yZA=='})

        b = BaseConnection(self.mock_url, token="iamnotarealtoken")
        headersOverlap(b.session.headers, {'Content-type': 'application/json', 'Authorization': 'Bearer iamnotarealtoken'})

        creds = {"OauthEmail": "fake.user@gmail.com",
                 "OauthAccessToken": "iamnotarealtoken",
                 "OauthExpires": 1551985931}

        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False)
        json.dump(creds, tmp)
        tmp.close()

        expected = creds.copy()
        expected["OauthExpires"] = str(expected["OauthExpires"])
        expected["Content-type"] = "application/json"
        b = BaseConnection(self.mock_url, credential_file=tmp.name)
        os.remove(tmp.name)
        headersOverlap(b.session.headers, expected)

        # test header propagation to Graph and Query classes
        c = Connection(self.mock_url, token="iamnotarealtoken")
        self.assertEqual(c.session.headers, c.graph('test').session.headers)
        self.assertEqual(c.session.headers, c.graph('test').query().session.headers)
