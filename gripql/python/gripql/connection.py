from __future__ import absolute_import, print_function, unicode_literals

import requests

from gripql.graph import Graph
from gripql.util import BaseConnection, raise_for_status


class Connection(BaseConnection):
    def __init__(self, url, user=None, password=None, token=None):
        super(Connection, self).__init__(url, user, password, token)
        self.url = self.base_url + "/v1/graph"

    def listGraphs(self):
        """
        List graphs.
        """
        response = requests.get(
            self.url,
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()['graphs']

    def addGraph(self, name):
        """
        Create a new graph.
        """
        response = requests.post(
            self.url + "/" + name,
            {},
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()

    def deleteGraph(self, name):
        """
        Delete graph.
        """
        response = requests.delete(
            self.url + "/" + name,
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()

    def getSchema(self, name):
        """
        Get a graph schema.
        """
        response = requests.get(
            self.url + "/" + name + "/schema",
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.base_url, name, self.user, self.password, self.token)
