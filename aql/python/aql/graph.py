from __future__ import absolute_import, print_function, unicode_literals

import json
import urllib2

from aql.util import do_request
from aql.query import Query


class Graph:
    def __init__(self, url, name):
        self.url = url
        self.name = name

    def addVertex(self, id, label, data={}):
        """
        Add vertex to a graph.
        """
        payload = json.dumps({
            "gid": id,
            "label": label,
            "data": data
        })
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + self.name + "/vertex",
                                  payload,
                                  headers=headers)
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def deleteVertex(self, gid):
        """
        Delete a vertex from the graph.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/vertex/" + gid
        request = urllib2.Request(url, headers=headers)
        request.get_method = lambda: "DELETE"
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def getVertex(self, gid):
        """
        Get a vertex by id.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/vertex/" + gid
        request = urllib2.Request(url, headers=headers)
        response = do_request(request)
        return json.loads(response.read())

    def addEdge(self, src, dst, label, data={}, id=None):
        """
        Add edge to the graph.
        """
        payload = {
            "from": src,
            "to": dst,
            "label": label,
            "data": data
        }
        if id is not None:
            payload["gid"] = id
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + self.name + "/edge",
                                  json.dumps(payload),
                                  headers=headers)
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def deleteEdge(self, gid):
        """
        Delete an edge from the graph.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + self.name + "/edge/" + gid,
                                  headers=headers)
        request.get_method = lambda: "DELETE"
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def getEdge(self, gid):
        """
        Get an edge by id.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/edge/" + gid
        request = urllib2.Request(url, headers=headers)
        response = do_request(request)
        return json.loads(response.read())

    def bulkAdd(self):
        return BulkAdd(self.url, self.name)

    def addIndex(self, label, field):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/index/" + label
        request = urllib2.Request(url,
                                  json.dumps({"field": field}),
                                  headers=headers)
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def listIndices(self):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/index"
        request = urllib2.Request(url, headers=headers)
        response = do_request(request)
        for result in response:
            d = json.loads(result)
            yield d

    def aggregate(self, aggregations):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        payload = {
            "aggregations": aggregations,
        }
        url = self.url + "/" + self.name + "/aggregate"
        request = urllib2.Request(url, json.dumps(payload), headers=headers)
        response = do_request(request)
        for result in response:
            d = json.loads(result)
            yield d["aggregations"]

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.url + "/" + self.name + "/query")


class BulkAdd:
    def __init__(self, url, graph):
        self.url = url
        self.graph = graph
        self.elements = []

    def addVertex(self, id, label, data={}):
        payload = json.dumps({
            "graph": self.graph,
            "vertex": {
                "gid": id,
                "label": label,
                "data": data
            }
        })
        self.elements.append(payload)

    def addEdge(self, src, dst, label, data={}):
        payload = json.dumps({
            "graph": self.graph,
            "edge": {
                "from": src,
                "to": dst,
                "label": label,
                "data": data
            }
        })
        self.elements.append(payload)

    def execute(self):
        payload = "\n".join(self.elements)
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = do_request(request)
        result = response.read()
        return json.loads(result)
