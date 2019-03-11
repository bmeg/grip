import unittest
import os

import logging
import http.client as http_client

from gripql.graph import Graph


def _dir():
    """Return the directory of this file."""
    return os.path.dirname(os.path.realpath(__file__))


def _path(*args):
    """Join args as path."""
    return os.path.join(*args)


class TestAuthHeader(unittest.TestCase):

    def test_no_params(self):
        g = Graph(url='foo', graph='name')
        assert g, 'should return a graph'

    def test_user_pass(self):
        g = Graph(url='url', graph='name', user='user', password='password')
        assert g, 'should return a graph'
        assert g._request_header() == {'Content-type': 'application/json', 'Authorization': 'Basic dXNlcjpwYXNzd29yZA=='}

    def test_credential_file(self):
        credential_file = _path(_dir(), 'fixture_credential_file.json')
        g = Graph(url='url', graph='name', credential_file=credential_file)
        assert g, 'should return a graph'
        assert sorted(g._request_header().keys()) == sorted(['OauthEmail', 'OauthAccessToken', 'OauthExpires', 'Content-type'])

    def test_auth(self):
        http_client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        credential_file = _path(_dir(), 'fixture_credential_file.json')
        url = 'http://localhost/api'
        g = Graph(url=url, graph='bmeg_test', credential_file=credential_file)
        assert g, 'should return a graph'
        try:
            g.listLabels()
        except Exception as e:
            print('expected error {}'.format(str(e)))
            with self.assertLogs('http.client.HTTPConnection', level='DEBUG') as logs:
                print(logs.output)


if __name__ == '__main__':
    unittest.main()
