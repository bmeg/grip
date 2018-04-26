import json
import urllib2


def and_(*expressions):
    return {"and": expressions}


def or_(*expressions):
    return {"or": expressions}


def not_(expression):
    return {"not": expression}


def eq(key, value):
    return {"key": key, "value": value, "condition": "EQ"}


def neq(key, value):
    return {"key": key, "value": value, "condition": "NEQ"}


def gt(key, value):
    return {"key": key, "value": value, "condition": "GT"}


def gte(key, value):
    return {"key": key, "value": value, "condition": "GTE"}


def lt(key, value):
    return {"key": key, "value": value, "condition": "LT"}


def lte(key, value):
    return {"key": key, "value": value, "condition": "LTE"}


def in_(key, values):
    if not isinstance(values, list):
        values = [values]
    return {"key": key, "value": values, "condition": "IN"}


def term(name, label, field, size=None):
    agg = {name: {"term": {"label": label, "field": field}}}
    if size:
        agg[name]["term"]["size"] = size
    return agg


def percentile(name, label, field, percents=[1, 5, 25, 50, 75, 95, 99]):
    return {
        name: {
            "percentile": {
                "label": label, "field": field, "percents": percents
            }
        }
    }


def histogram(name, label, field, interval):
    return {
        name: {
            "percentile": {
                "label": label, "field": field, "interval": interval
            }
        }
    }


class Connection:
    def __init__(self, url):
        scheme, netloc, path, query, frag = urllib2.urlparse.urlsplit(url)
        query = ""
        frag = ""
        if scheme == "":
            scheme = "http"
        if netloc == "" and path != "":
            netloc = path
            path = ""
        host = urllib2.urlparse.urlunsplit((scheme, netloc, path, query, frag))
        self.host = host
        self.url = "%s/v1/graph" % (host)

    def list(self):
        """
        List graphs.
        """
        request = urllib2.Request(self.url)
        response = urllib2.urlopen(request)
        txt = response.read()
        if len(txt) == 0:
            return []
        lines = txt.rstrip().split("\n")
        out = []
        for i in lines:
            out.append(json.loads(i))
        return out

    def new(self, name):
        """
        New graph.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + name, "{}", headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def delete(self, name):
        """
        Delete graph.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + name, headers=headers)
        request.get_method = lambda: "DELETE"
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.url, name)


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
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

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
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addSubGraph(self, graph):
        payload = json.dumps(graph)
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url + "/" + self.name + "/subgraph",
                                  payload,
                                  headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def bulkAdd(self):
        return BulkAdd(self.url, self.name)

    def addIndex(self, label, field):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/index/" + label
        request = urllib2.Request(url,
                                  json.dumps({"field": field}),
                                  headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def listIndices(self):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/index"
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
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
        response = urllib2.urlopen(request)
        for result in response:
            d = json.loads(result)
            yield d

    def getVertex(self, gid):
        """
        Get a vertex by id.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/vertex/" + gid
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        return json.loads(response.read())

    def getEdge(self, gid):
        """
        Get an edge by id.
        """
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        url = self.url + "/" + self.name + "/edge/" + gid
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        return json.loads(response.read())

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.url + "/" + self.name + "/query")

    def mark(self, name):
        """
        Create mark step for match query
        """
        return self.query().mark(name)


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

    def commit(self):
        payload = "\n".join(self.elements)
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)


class Query:
    def __init__(self, url):
        self.query = []
        self.url = url

    def __append(self, part):
        q = Query(self.url)
        q.query = self.query[:]
        q.query.append(part)
        return q

    def V(self, id=[]):
        """
        Start the query at a vertex.

        "id" is an ID or a list of vertex IDs to start from. Optional.
        """
        if not isinstance(id, list):
            id = [id]
        return self.__append({"v": id})

    def E(self, id=[]):
        """
        Start the query at an edge.

        "id" is an ID to start from. Optional.
        """
        if not isinstance(id, list):
            id = [id]
        return self.__append({"e": id})

    def where(self, expressions):
        """
        Filter vertex/edge based on properties.
        """
        if not isinstance(expressions, list):
            expressions = [expressions]
        return self.__append({"where": {"expressions": expressions}})

    def fields(self, fields=[]):
        """
        Select document properties to be returned in document.
        """
        if not isinstance(fields, list):
            fields = [fields]
        return self.__append({"fields": fields})

    def incoming(self, label=[]):
        """
        Follow an incoming edge to the source vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"in": label})

    def outgoing(self, label=[]):
        """
        Follow an outgoing edge to the destination vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"out": label})

    def both(self, label=[]):
        """
        Follow both incoming and outgoing edges to vertices.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"both": label})

    def incomingEdge(self, label=[]):
        """
        Move from a vertex to an incoming edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"in_edge": label})

    def outgoingEdge(self, label=[]):
        """
        Move from a vertex to an outgoing edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"out_edge": label})

    def bothEdge(self, label=[]):
        """
        Move from a vertex to incoming/outgoing edges.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        return self.__append({"both_edge": label})

    def mark(self, name):
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
        if not isinstance(marks, list):
            marks = [marks]
        return self.__append({"select": {"labels": marks}})

    def limit(self, n):
        """
        Limits the number of results returned.
        """
        return self.__append({"limit": n})

    def count(self):
        """
        Return the number of results, instead of the elements.
        """
        return self.__append({"count": ""})

    def groupCount(self, label):
        """
        Group results by the given property name and count each group.
        """
        return self.__append({"group_count": label})

    def distinct(self, props=[]):
        """
        Select distinct elements based on the provided property list.
        """
        if not isinstance(props, list):
            props = [props]
        return self.__append({"distinct": props})

    def jsImport(self, src):
        """
        Initialize javascript engine with functions and global variables.
        """
        return self.__append({"import": src})

    def map(self, func):
        """
        Transform results by the given javascript function.
        function(el) el
        """
        return self.__append({"map": func})

    def filter(self, func):
        """
        Filter results by the given javascript function.
        function(el) bool
        """
        return self.__append({"filter": func})

    def fold(self, init, func):
        """
        Fold results by the given javascript function.
        function(el) el
        """
        return self.__append({"fold": {"init": init, "source": func}})

    def match(self, queries):
        """
        Intersect multiple queries.
        """
        mq = []
        for i in queries:
            mq.append({"query": i.query})
        return self.__append({"match": {"queries": mq}})

    def render(self, template):
        """
        Render output of query
        """
        self.query.append({"render": template})
        return self

    def string(self):
        """
        Return the query as a JSON string.
        """
        output = {"query": self.query}
        return json.dumps(output)

    def __iter__(self):
        return self.execute()

    def execute(self):
        """
        Execute the query and return an iterator.
        """
        payload = self.string()
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        for result in response:
            try:
                d = json.loads(result)
                if "value" in d:
                    yield d["value"]
                elif "row" in d:
                    yield d["row"]
            except ValueError as e:
                print("Can't decode: %s" % (result))
                raise e

    def first(self):
        """
        Return only the first result.
        """
        return list(self.execute())[0]
