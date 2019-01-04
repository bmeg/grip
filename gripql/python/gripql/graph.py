from __future__ import absolute_import, print_function, unicode_literals

import os
import json
import requests

from gripql.util import process_url, raise_for_status
from gripql.query import Query


class Graph:
    def __init__(self, url, name, user=None, password=None):
        url = process_url(url)
        self.base_url = url
        self.url = url + "/v1/graph/" + name
        self.name = name
        if user is None:
            user = os.getenv("GRIP_USER", None)
        self.user = user
        if password is None:
            password = os.getenv("GRIP_PASSWORD", None)
        self.password = password

    def addVertex(self, gid, label, data={}):
        """
        Add vertex to a graph.
        """
        payload = {
            "gid": gid,
            "label": label,
            "data": data
        }
        response = requests.post(
            self.url + "/vertex",
            json=payload,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def deleteVertex(self, gid):
        """
        Delete a vertex from the graph.
        """
        url = self.url + "/vertex/" + gid
        response = requests.delete(
            url,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def getVertex(self, gid):
        """
        Get a vertex by id.
        """
        url = self.url + "/vertex/" + gid
        response = requests.get(
            url,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def addEdge(self, src, dst, label, data={}, gid=None):
        """
        Add edge to the graph.
        """
        payload = {
            "from": src,
            "to": dst,
            "label": label,
            "data": data
        }
        if gid is not None:
            payload["gid"] = gid
        response = requests.post(
            self.url + "/edge",
            json=payload,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def deleteEdge(self, gid):
        """
        Delete an edge from the graph.
        """
        url = self.url + "/edge/" + gid
        response = requests.delete(
            url,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def getEdge(self, gid):
        """
        Get an edge by id.
        """
        url = self.url + "/edge/" + gid
        response = requests.get(
            url,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def bulkAdd(self):
        return BulkAdd(self.base_url, self.name, self.user, self.password)

    def addIndex(self, label, field):
        url = self.url + "/index/" + label
        response = requests.post(
            url,
            json={"field": field},
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def listIndices(self):
        url = self.url + "/index"
        response = requests.get(
            url,
            stream=True,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()["indices"]

    def listLabels(self):
        url = self.url + "/label"
        response = requests.get(
            url,
            stream=True,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()

    def aggregate(self, aggregations):
        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        payload = {
            "aggregations": aggregations,
        }
        url = self.url + "/aggregate"
        response = requests.post(
            url,
            json=payload,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()["aggregations"]

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.base_url, self.name)


class BulkAdd:
    def __init__(self, url, graph, user=None, password=None):
        url = process_url(url)
        self.base_url = url
        self.url = url + "/v1/graph"
        self.graph = graph
        self.elements = []
        if user is None:
            user = os.getenv("GRIP_USER", None)
        self.user = user
        if password is None:
            password = os.getenv("GRIP_PASSWORD", None)
        self.password = password

    def addVertex(self, gid, label, data={}):
        payload = {
            "graph": self.graph,
            "vertex": {
                "gid": gid,
                "label": label,
                "data": data
            }
        }
        self.elements.append(json.dumps(payload))

    def addEdge(self, src, dst, label, data={}, gid=None):
        payload = {
            "graph": self.graph,
            "edge": {
                "from": src,
                "to": dst,
                "label": label,
                "data": data
            }
        }
        if gid is not None:
            payload["gid"] = gid
        self.elements.append(json.dumps(payload))

    def execute(self):
        payload = "\n".join(self.elements)
        response = requests.post(
            self.url,
            data=payload,
            auth=(self.user, self.password)
        )
        raise_for_status(response)
        return response.json()
