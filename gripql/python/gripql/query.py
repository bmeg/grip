from __future__ import absolute_import, print_function, unicode_literals

import json
import logging

from gripql.util import AttrDict, BaseConnection, Rate, raise_for_status


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


class Query(BaseConnection):
    def __init__(self, url, graph, user=None, password=None, token=None, credential_file=None):
        super(Query, self).__init__(url, user, password, token, credential_file)
        self.url = self.base_url + "/v1/graph/" + graph + "/query"
        self.graph = graph
        self.query = []

    def __append(self, part):
        q = self.__class__(self.base_url, self.graph, self.user, self.password, self.token, self.credential_file)
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

    def in_(self, label=[]):
        """
        Follow an incoming edge to the source vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"in": label})

    def inV(self, label=[]):
        return self.in_(label)

    def out(self, label=[]):
        """
        Follow an outgoing edge to the destination vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"out": label})

    def outV(self, label=[]):
        return self.out(label)

    def both(self, label=[]):
        """
        Follow both incoming and outgoing edges to vertices.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        label = _wrap_str_value(label)
        return self.__append({"both": label})

    def bothV(self, label=[]):
        return self.both(label)

    def inE(self, label=[]):
        """
        Move from a vertex to an incoming edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"in_e": label})

    def outE(self, label=[]):
        """
        Move from a vertex to an outgoing edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"out_e": label})

    def bothE(self, label=[]):
        """
        Move from a vertex to incoming/outgoing edges.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        label = _wrap_str_value(label)
        return self.__append({"both_e": label})

    def has(self, expression):
        """
        Filter vertex/edge based on properties.
        """
        return self.__append({"has": expression})

    def hasLabel(self, label):
        """
        Filter vertex/edge based on label.
        """
        label = _wrap_str_value(label)
        return self.__append({"hasLabel": label})

    def hasId(self, id):
        """
        Filter vertex/edge based on id.
        """
        id = _wrap_str_value(id)
        return self.__append({"hasId": id})

    def hasKey(self, key):
        """
        Filter vertex/edge based on the existence of properties.
        """
        key = _wrap_str_value(key)
        return self.__append({"hasKey": key})

    def fields(self, field=[]):
        """
        Select document properties to be returned in document.
        """
        field = _wrap_str_value(field)
        return self.__append({"fields": field})

    def as_(self, name):
        """
        Mark the current vertex/edge with the given name.

        Used to return elements from select().
        """
        return self.__append({"as": name})

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

    def skip(self, n):
        """
        Offset the results returned.
        """
        return self.__append({"skip": n})

    def range(self, offset, limit):
        """
        Offset and limit the results returned.
        """
        return self.__append({"range": {"start": offset, "stop": limit}})

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
        response = self.session.post(
            self.url,
            json=self.to_dict(),
            stream=True
        )
        logger.debug('POST %s', self.url)
        logger.debug('BODY %s', self.to_json())
        logger.debug('STATUS CODE %s', response.status_code)
        raise_for_status(response)

        for result in response.iter_lines(chunk_size=None):
            try:
                result_dict = json.loads(result.decode())
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


class __Query(Query):
    def __init__(self):
        self.query = []

    def __append(self, part):
        q = self.__class__()
        q.query = self.query[:]
        q.query.append(part)
        return q

    def __iter__(self):
        pass

    def __stream(self):
        pass

    def execute(self):
        pass


__ = __Query()
