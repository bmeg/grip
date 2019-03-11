from __future__ import absolute_import, print_function, unicode_literals

import json

from gripql.util import BaseConnection, raise_for_status
from gripql.query import Query


class Graph(BaseConnection):
    def __init__(self, url, graph, user=None, password=None, token=None, credential_file=None):
        super(Graph, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/graph/" + graph
        self.graph = graph

    def addSchema(self, vertices=[], edges=[]):
        """
        Add vertex to a graph.
        """
        payload = {
            "graph": self.graph,
            "vertices": vertices,
            "edges": edges
        }
        response = self.session.post(
            self.url + "/schema",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def addVertex(self, gid, label, data={}):
        """
        Add vertex to a graph.
        """
        payload = {
            "gid": gid,
            "label": label,
            "data": data
        }
        response = self.session.post(
            self.url + "/vertex",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def deleteVertex(self, gid):
        """
        Delete a vertex from the graph.
        """
        url = self.url + "/vertex/" + gid
        response = self.session.delete(
            url
        )
        raise_for_status(response)
        return response.json()

    def getVertex(self, gid):
        """
        Get a vertex by id.
        """
        url = self.url + "/vertex/" + gid
        response = self.session.get(
            url
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
        response = self.session.post(
            self.url + "/edge",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def deleteEdge(self, gid):
        """
        Delete an edge from the graph.
        """
        url = self.url + "/edge/" + gid
        response = self.session.delete(
            url
        )
        raise_for_status(response)
        return response.json()

    def getEdge(self, gid):
        """
        Get an edge by id.
        """
        url = self.url + "/edge/" + gid
        response = self.session.get(
            url
        )
        raise_for_status(response)
        return response.json()

    def bulkAdd(self):
        return BulkAdd(self.base_url, self.graph, self.user, self.password, self.token)

    def addIndex(self, label, field):
        url = self.url + "/index/" + label
        response = self.session.post(
            url,
            json={"field": field}
        )
        raise_for_status(response)
        return response.json()

    def listIndices(self):
        url = self.url + "/index"
        response = self.session.get(
            url,
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()["indices"]

    def listLabels(self):
        url = self.url + "/label"
        response = self.session.get(
            url
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
        response = self.session.post(
            url,
            json=payload
        )
        raise_for_status(response)
        return response.json()["aggregations"]

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.base_url, self.graph, self.user, self.password, self.token, self.credential_file)


class BulkAdd(BaseConnection):
    def __init__(self, url, graph, user=None, password=None, token=None, credential_file=None):
        super(BulkAdd, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/graph"
        self.graph = graph
        self.elements = []

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
        response = self.session.post(
            self.url,
            data=payload
        )
        raise_for_status(response)
        return response.json()
