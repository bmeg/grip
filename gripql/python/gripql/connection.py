from __future__ import absolute_import, print_function, unicode_literals

import os
import requests

from gripql.graph import Graph
from gripql.util import process_url, raise_for_status


class Connection:
    def __init__(self, url, user=None, password=None):
        url = process_url(url)
        self.base_url = url
        self.url = url + "/v1/graph"
        if user is None:
            user = os.getenv("GRIP_USER", None)
        self.user = user
        if password is None:
            password = os.getenv("GRIP_PASSWORD", None)
        self.password = password

    def listGraphs(self):
        """
        List graphs.
        """
        response = requests.get(
            self.url,
            auth=(self.user, self.password)
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
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def deleteGraph(self, name):
        """
        Delete graph.
        """
        response = requests.delete(
            self.url + "/" + name,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def getSchema(self, name):
        """
        Get a graph schema.
        """
        response = requests.get(
            self.url + "/" + name + "/schema",
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.base_url, name, self.user, self.password)
