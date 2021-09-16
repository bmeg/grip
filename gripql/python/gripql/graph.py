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
        Add the schema for a graph.
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

    def getSchema(self):
        """
        Get the schema for a graph.
        """
        response = self.session.get(
            self.url + "/schema"
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

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.base_url, self.graph, self.user, self.password, self.token, self.credential_file)

    def resume(self, job_id):
        """
        Create a query handle.
        """
        return Query(self.base_url, self.graph, self.user, self.password, self.token, self.credential_file, resume=job_id)

    def listJobs(self):
        url = self.url + "/job"
        response = self.session.get(
            url,
            headers=self._request_header()
        )
        for result in response.iter_lines(chunk_size=None):
            yield json.loads(result)

    def getJob(self, id):
        """
        get job
        """
        response = self.session.get(
            self.url + "/job/" + id,
            headers=self._request_header()
        )
        raise_for_status(response)
        return response.json()

    def deleteJob(self, id):
        """
        Delete an job
        """
        url = self.url + "/job/" + id
        response = self.session.delete(
            url
        )
        raise_for_status(response)
        return response.json()

    def readJob(self, id, raw=False):
        """
        read job
        """
        response = self.session.post(
            self.url + "/job/" + id,
            json={},
            headers=self._request_header()
        )
        # Duplicate code from Query, need to get helper function
        for result in response.iter_lines(chunk_size=None):
            try:
                result_dict = json.loads(result.decode())
            except Exception as e:
                #logger.error("Failed to decode: %s", result)
                raise e

            if raw:
                extracted = result_dict
            elif "vertex" in result_dict:
                extracted = result_dict["vertex"]
            elif "edge" in result_dict:
                extracted = result_dict["edge"]
            elif "aggregations" in result_dict:
                extracted = result_dict["aggregations"]
            elif "selections" in result_dict:
                extracted = result_dict["selections"]["selections"]
                for k in extracted:
                    if "vertex" in extracted[k]:
                        extracted[k] = extracted[k]["vertex"]
                    elif "edge" in extracted[k]:
                        extracted[k] = extracted[k]["edge"]
            elif "render" in result_dict:
                extracted = result_dict["render"]
            elif "count" in result_dict:
                extracted = result_dict
            elif "error" in result_dict:
                raise requests.HTTPError(result_dict['error']['message'])
            else:
                extracted = result_dict

            yield extracted


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
