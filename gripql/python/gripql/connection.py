from __future__ import absolute_import, print_function, unicode_literals

from gripql.graph import Graph
from gripql.util import BaseConnection, raise_for_status


class Connection(BaseConnection):
    def __init__(self, url, user=None, password=None, token=None, credential_file=None):
        super(Connection, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/graph"

    def listGraphs(self):
        """
        List graphs.
        """
        response = self.session.get(
            self.url
        )
        raise_for_status(response)
        return response.json()['graphs']

    def addGraph(self, name):
        """
        Create a new graph.
        """
        response = self.session.post(
            self.url + "/" + name,
            {}
        )
        raise_for_status(response)
        return response.json()

    def deleteGraph(self, name):
        """
        Delete graph.
        """
        response = self.session.delete(
            self.url + "/" + name
        )
        raise_for_status(response)
        return response.json()

    def getSchema(self, name):
        """
        Get a graph schema.
        """
        response = self.session.get(
            self.url + "/" + name + "/schema"
        )
        raise_for_status(response)
        return response.json()

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.base_url, name, self.user, self.password, self.token, self.credential_file)
