from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
import os
import requests

from gripql.util import AttrDict, Rate, process_url


def _wrap_value(value, typ):
    wrapped = []
    if isinstance(value, list):
        if not all(isinstance(i, typ) for i in value):
            raise TypeError("expected all values in array to be a string")
        wrapped = value
    elif isinstance(value, typ):
        wrapped.append(value)
    elif value is None:
        pass
    else:
        raise TypeError("expected value to be a %s" % typ)
    return wrapped


def _wrap_str_value(value):
    return _wrap_value(value, str)


def _wrap_dict_value(value):
    return _wrap_value(value, dict)


class Query:
    def __init__(self, url, graph, user=None, password=None):
        self.query = []
        url = process_url(url)
        self.base_url = url
        self.url = url + "/v1/graph/" + graph + "/query"
        self.graph = graph
        if user is None:
            user = os.getenv("GRIP_USER", None)
        self.user = user
        if password is None:
            password = os.getenv("GRIP_PASSWORD", None)
        self.password = password

    def __append(self, part):
        q = self.__class__(self.base_url, self.graph)
        q.query = self.query[:]
        q.query.append(part)
        return q

    def V(self, id=[]):
        """
        Start the query at a vertex.

        "id" is an ID or a list of vertex IDs to start from. Optional.
        """
        id = _wrap_str_value(id)
        return self.__append({"v": id})

    def E(self, id=[]):
        """
        Start the query at an edge.

        "id" is an ID to start from. Optional.
        """
        id = _wrap_str_value(id)
        return self.__append({"e": id})

    def where(self, expression):
        """
        Filter vertex/edge based on properties.
        """
        return self.__append({"where": expression})

    def fields(self, field=[]):
        """
        Select document properties to be returned in document.
        """
        field = _wrap_str_value(field)
        return self.__append({"fields": field})

    def in_(self, label=[]):
        """
        Follow an incoming edge to the source vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"in": label})

    def out(self, label=[]):
        """
        Follow an outgoing edge to the destination vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"out": label})

    def both(self, label=[]):
        """
        Follow both incoming and outgoing edges to vertices.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"both": label})

    def inEdge(self, label=[]):
        """
        Move from a vertex to an incoming edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"in_edge": label})

    def outEdge(self, label=[]):
        """
        Move from a vertex to an outgoing edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"out_edge": label})

    def bothEdge(self, label=[]):
        """
        Move from a vertex to incoming/outgoing edges.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"both_edge": label})

    def mark(self, name):
        """
        Mark the current vertex/edge with the given name.

        Used to return elements from select().
        """
        return self.__append({"mark": name})

    def select(self, marks):
        """
        Returns rows of marked elements, with one item for each mark.

        "marks" is a list of mark names.
        The rows returned are all combinations of marks, e.g.
        [
            [A1, B1],
            [A1, B2],
            [A2, B1],
            [A2, B2],
        ]
        """
        marks = _wrap_str_value(marks)
        return self.__append({"select": {"marks": marks}})

    def limit(self, n):
        """
        Limits the number of results returned.
        """
        return self.__append({"limit": n})

    def offset(self, n):
        """
        Offset the results returned.
        """
        return self.__append({"offset": n})

    def count(self):
        """
        Return the number of results, instead of the elements.
        """
        return self.__append({"count": ""})

    def distinct(self, props=[]):
        """
        Select distinct elements based on the provided property list.
        """
        props = _wrap_str_value(props)
        return self.__append({"distinct": props})

    def match(self, queries):
        """
        Intersect multiple queries.
        """
        if not isinstance(queries, list):
            raise TypeError("match expects an array")
        if not all(isinstance(i, Query) for i in queries):
            raise TypeError("expected all aruments to match to be a \
            Query instance")
        mq = []
        for i in queries:
            mq.append({"query": i.query})
        return self.__append({"match": {"queries": mq}})

    def render(self, template):
        """
        Render output of query
        """
        return self.__append({"render": template})

    def aggregate(self, aggregations):
        """
        Aggregate results of query output
        """
        aggregations = _wrap_dict_value(aggregations)
        return self.__append({"aggregate": {"aggregations": aggregations}})

    def to_json(self):
        """
        Return the query as a JSON string.
        """
        output = {"query": self.query}
        return json.dumps(output)

    def to_dict(self):
        """
        Return the query as a dictionary.
        """
        return {"query": self.query}

    def __iter__(self):
        return self.__stream()

    def __stream(self, debug=False):
        """
        Execute the query and return an iterator.
        """
        log_level = logging.INFO
        if debug:
            log_level = logging.DEBUG
        logger = logging.getLogger(__name__)
        logger.handlers = []
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter('[%(levelname)s]\t%(asctime)s\t%(message)s')
        )
        stream_handler.setLevel(log_level)
        logger.setLevel(log_level)
        logger.addHandler(stream_handler)

        rate = Rate(logger)
        rate.init()
        response = requests.post(
            self.url,
            json=self.to_dict(),
            stream=True,
            auth=(self.user, self.password)
        )
        logger.debug('POST %s', self.url)
        logger.debug("BODY %s", self.to_json())
        logger.debug('STATUS CODE %s', response.status_code)
        response.raise_for_status()

        for result in response.iter_lines(chunk_size=None):
            try:
                result_dict = json.loads(result)
            except Exception as e:
                logger.error("Failed to decode: %s", result)
                raise e

            if "vertex" in result_dict:
                extracted = result_dict["vertex"]
            elif "edge" in result_dict:
                extracted = result_dict["edge"]
            elif "aggregations" in result_dict:
                extracted = result_dict["aggregations"]["aggregations"]
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
            else:
                extracted = result_dict

            if isinstance(extracted, dict):
                yield AttrDict(extracted)
            else:
                yield extracted

            rate.tick()
        rate.close()

    def execute(self, stream=False, debug=False):
        """
        Execute the query.

        If stream is True an iterator will be returned. Otherwise, a list
        is returned.
        """
        if stream:
            return self.__stream(debug)
        else:
            output = []
            for r in self.__stream(debug):
                output.append(r)
            return output
