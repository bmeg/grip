from __future__ import absolute_import, print_function, unicode_literals

import os
import json
from gripql.graph import Graph
from gripql.util import BaseConnection, raise_for_status


class Connection(BaseConnection):
    def __init__(self, url=None, user=None, password=None, token=None, credential_file=None):
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

    def postSchema(self, name, vertices, edges):
        """
        Post a graph schema
        """
        response = self.session.post(
            self.url + "/" + name + "/schema",
            {"vertices" : vertices, "edges" : edges}
        )
        raise_for_status(response)
        return response.json()

    def getMapping(self, name):
        """
        Get a graph mapping.
        """
        response = self.session.get(
            self.url + "/" + name + "/mapping"
        )
        raise_for_status(response)
        return response.json()

    def postMapping(self, name, vertices, edges):
        """
        Post a graph mapping
        """
        response = self.session.post(
            self.url + "/" + name + "/mapping",
            json={"vertices" : vertices, "edges" : edges}
        )
        #raise_for_status(response)
        print("mapping", response.text)
        return response.json()

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.base_url, name, self.user, self.password, self.token, self.credential_file)

    def listTables(self):
        """
        List graphs.
        """
        response = self.session.get(
            self.base_url + "/v1/table"
        )
        raise_for_status(response)
        for line in response.iter_lines(chunk_size=None):
            yield json.loads(line)

    def listDrivers(self):
        """
        List graphs.
        """
        response = self.session.get(
            self.base_url + "/v1/driver"
        )
        raise_for_status(response)
        return response.json()['drivers']

    def listPlugins(self):
        """
        List graphs.
        """
        response = self.session.get(
            self.base_url + "/v1/plugin"
        )
        raise_for_status(response)
        return response.json()['plugins']

    def startPlugin(self, name, driver, config):
        response = self.session.post(
            self.base_url + "/v1/plugin/" + name,
            json={"driver":driver, "config":config}
        )
        return response.json()
