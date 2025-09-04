from __future__ import absolute_import, print_function, unicode_literals

import json
import requests

from gripql.util import BaseConnection, raise_for_status
from gripql.query import Query


class Graph(BaseConnection):
    def __init__(self, url, graph, user=None, password=None, token=None, credential_file=None):
        super(Graph, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/graph/" + graph
        self.graph = graph

    # This method is what makes it possible to call query methods directly from a Graph object.
    def __getattr__(self, name):
        """
        Dynamically handles method calls that don't exist in Graph.
        If the method exists in the Query class, it is called on a new Query object.
        """
        # Create a new Query object
        q = Query(self.base_url, self.graph, self.user, self.password, self.token, self.credential_file)

        # Check if the requested method exists in the Query class
        if hasattr(q, name) and callable(getattr(q, name)):
            # Return a wrapper function that calls the method on the Query object
            def method_wrapper(*args, **kwargs):
                return getattr(q, name)(*args, **kwargs)
            return method_wrapper

        # If the method is not found, raise the default AttributeError
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


    def addJsonSchema(self, fhirjson):
       """
       Add a Json Schema for a graph
       """
       payload = {
           "graph": self.graph,
           "data":fhirjson,
       }
       response = self.session.post(
           self.url + "/jsonschema",
           json=payload
       )
       raise_for_status(response)
       return response.json()

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

    def sampleSchema(self):
        """
        Get the schema for a graph.
        """
        response = self.session.get(
            self.url + "/schema-sample"
        )
        raise_for_status(response)
        return response.json()

    def addVertex(self, id, label, data={}):
        """
        Add vertex to a graph.
        """
        payload = {
            "id": id,
            "label": label,
            "data": data
        }
        response = self.session.post(
            self.url + "/vertex",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def deleteVertex(self, id):
        """
        Delete a vertex from the graph.
        """
        url = self.url + "/vertex/" + id
        response = self.session.delete(
            url
        )
        raise_for_status(response)
        return response.json()

    def getVertex(self, id):
        """
        Get a vertex by id.
        """
        url = self.url + "/vertex/" + id
        response = self.session.get(
            url
        )
        raise_for_status(response)
        return response.json()

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
            payload["id"] = id
        response = self.session.post(
            self.url + "/edge",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def deleteEdge(self, id):
        """
        Delete an edge from the graph.
        """
        url = self.url + "/edge/" + id
        response = self.session.delete(
            url
        )
        raise_for_status(response)
        return response.json()

    def getEdge(self, id):
        """
        Get an edge by id.
        """
        url = self.url + "/edge/" + id
        response = self.session.get(
            url
        )
        raise_for_status(response)
        return response.json()

    def delete(self, vertices=[], edges=[]):
        """
        delete data from graph
        """
        payload = {
            "graph": self.graph,
            "vertices": vertices,
            "edges": edges
        }
        response = self.session.delete(
            self.base_url + "/v1/graph",
            json=payload
        )
        raise_for_status(response)
        return response.json()

    def bulkAdd(self):
        return BulkAdd(self.base_url, self.graph, self.user, self.password, self.token)

    def bulkAddRaw(self):
        return BulkAddRaw(self.base_url, self.graph, self.user, self.password, self.token)

    def addIndex(self, label, field):
        url = self.url + "/index/" + label
        response = self.session.post(
            url,
            json={"field": field}
        )
        raise_for_status(response)
        return response.json()

    def deleteIndex(self, label, field):
        url = self.url + f"/index/{label}/{field}"
        response = self.session.delete(
            url,
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

    def addVertex(self, id, label, data={}):
        payload = {
            "graph": self.graph,
            "vertex": {
                "id": id,
                "label": label,
                "data": data
            }
        }
        self.elements.append(json.dumps(payload))

    def addEdge(self, src, dst, label, data={}, id=None):
        payload = {
            "graph": self.graph,
            "edge": {
                "from": src,
                "to": dst,
                "label": label,
                "data": data
            }
        }
        if id is not None:
            payload["edge"]["id"] = id
        self.elements.append(json.dumps(payload))

    def execute(self):
        payload = "\n".join(self.elements)
        response = self.session.post(
            self.url,
            data=payload
        )
        raise_for_status(response)
        return response.json()


class BulkAddRaw(BaseConnection):
    def __init__(self, url, graph, extraArgs=None, user=None, password=None, token=None, credential_file=None):
        super(BulkAddRaw, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/rawJson"
        self.graph = graph
        self.elements = []


    def addJson(self, data={}, extra_args={}):
        payload = {
            "graph": self.graph,
            "extra_args": extra_args,
            "data": data
        }
        self.elements.append(json.dumps(payload))


    def execute(self):
        payload = "\n".join(self.elements)
        response = self.session.post(
            self.url,
            data=payload
        )
        raise_for_status(response)
        return response.json()
