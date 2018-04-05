import json
import urllib2


class Connection:
    def __init__(self, host):
        self.host = host
        self.url =  "%s/v1/graph" % (host)

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
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request("%s/%s" % (self.url, name), "{}", headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def delete(self, name):
        """
        Delete graph.
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request("%s/%s" % (self.url, name), headers=headers)
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

    def query(self):
        """
        Create a query handle.
        """
        return Query(self.url + "/" + self.name + "/query")

    def addVertex(self, id, label, data={}):
        """
        Add vertex to a graph.
        """
        payload = json.dumps({
            "gid" : id,
            "label" : label,
            "data" : data
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/vertex", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addEdge(self, src, dst, label, data={}, id=None):
        """
        Add edge to the graph.
        """
        payload = {
            "from" : src,
            "to" : dst,
            "label" : label,
            "data" : data
        }
        if id is not None:
            payload['gid'] = id
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/edge", json.dumps(payload), headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addSubGraph(self, graph):
        payload = json.dumps(graph)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/subgraph", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addBundle(self, src, bundle, label):
        payload = json.dumps({
            "from" : src,
            "bundle" : bundle,
            "label" : label,
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/bundle", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def bulkAdd(self):
        return BulkAdd(self.url, self.name)

    def addVertexIndex(self, label, field):
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = self.url + "/" + self.name + "/index/" + label
        request = urllib2.Request(url, json.dumps({"field":field}), headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def listVertexList(self):
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = self.url + "/" + self.name + "/index"
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        for result in response:
            d = json.loads(result)
            yield d

    def index(self):
        """
        Create a index handle.
        """
        return Index(self)

    def mark(self, name):
        """
        Create mark step for match query
        """
        q = self.query()
        q.mark(name)
        return q


class BulkAdd:
    def __init__(self, url, graph):
        self.url = url
        self.graph = graph
        self.elements = []

    def addVertex(self, id, label, data={}):
        payload = json.dumps({
            "graph" : self.graph,
            "vertex" : {
                "gid" : id,
                "label" : label,
                "data" : data
            }
        })
        self.elements.append(payload)

    def addEdge(self, src, dst, label, data={}):
        payload = json.dumps({
            "graph" : self.graph,
            "edge" : {
                "from" : src,
                "to" : dst,
                "label" : label,
                "data" : data
            }
        })
        self.elements.append(payload)

    def commit(self):
        payload = "\n".join(self.elements)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)


class Index:
    def __init__(self, parent=None):
        self.parent = parent

    def getVertexIndex(self, label, field):
        url = self.parent.url + "/" + self.parent.name + "/index/" + label + "/" + field
        request = urllib2.Request(url)
        response = urllib2.urlopen(request)
        for result in response:
            d = json.loads(result)
            yield d

    def query(self, label, field, value):
        url = self.parent.url + "/" + self.parent.name + "/index/" + label + "/" + field
        return Query(url)


class Query:
    def __init__(self, url):
        self.query = []
        self.url = url

    def js_import(self, src):
        """
        Initialize javascript engine with functions and global variables.
        """
        self.query.append({"import":src})
        return self

    def V(self, id=[]):
        """
        Start the query at a vertex.

        "id" is an ID or a list of vertex IDs to start from. Optional.
        """
        if not isinstance(id, list):
            id = [id]
        self.query.append({"v":id})
        return self

    def E(self, id=[]):
        """
        Start the query at an edge.

        "id" is an ID to start from. Optional.
        """
        if not isinstance(id, list):
            id = [id]
        self.query.append({"e":id})
        return self

    def hasLabel(self, label):
        """
        Match vertex/edge label.

        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'has_label': label})
        return self

    def hasId(self, id):
        """
        Match vertex/edge ID.

        "id" can be a list.
        """
        if not isinstance(id, list):
            id = [id]
        self.query.append({'has_id': id})
        return self

    def has(self, key, value):
        """
        Match vertex/edge property.

        If "value" is a list, then data must match at least one item.
        """
        if not isinstance(value, list):
            value = [value]
        self.query.append({'has': { "key" : key, 'within': value}})
        return self

    def values(self, v):
        """
        Extract document properties into returned document.
        """
        if not isinstance(v, list):
            v = [v]
        self.query.append({'values': {"labels" : v}})
        return self

    def incoming(self, label=[]):
        """
        Follow an incoming edge to the source vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'in': label})
        return self

    def outgoing(self, label=[]):
        """
        Follow an outgoing edge to the destination vertex.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'out': label})
        return self

    def both(self, label=[]):
        """
        Follow both incoming and outgoing edges to vertices.

        "label" is the label of the edge to follow.
        "label" can be a list.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'both': label})
        return self

    def incomingEdge(self, label=[]):
        """
        Move from a vertex to an incoming edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'in_edge': label})
        return self

    def outgoingEdge(self, label=[]):
        """
        Move from a vertex to an outgoing edge.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'out_edge': label})
        return self

    def bothEdge(self, label=[]):
        """
        Move from a vertex to incoming/outgoing edges.

        "label" is the label of the edge to move to.
        "label" can be a list.

        Must be called from a vertex.
        """
        if not isinstance(label, list):
            label = [label]
        self.query.append({'both_edge': label})
        return self

    def outgoingBundle(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'out_bundle': label})
        return self

    def mark(self, name):
        """
        Mark the current vertex/edge with the given name.

        Used to return elements from select().
        """
        self.query.append({'as': name})
        return self

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
        self.query.append({'select': {"labels" : marks}})
        return self

    def limit(self, l):
        """
        Limits the number of results returned.
        """
        self.query.append({'limit': l})
        return self

    def range(self, begin, end):
        """
        """
        self.query.append({'begin': begin, 'end': end})
        return self

    def count(self):
        """
        Return the number of results, instead of the elements.
        """
        self.query.append({'count': ''})
        return self

    def groupCount(self, label):
        """
        Group results by the given property name and count each group.
        """
        self.query.append({'group_count': label})
        return self

    def distinct(self, val):
        """
        So distinct elements
        """
        if not isinstance(val, list):
            val = [val]
        self.query.append({"distinct" : val})
        return self

    def map(self, func):
        """
        Transform results by the given javascript function.
        function(el) el
        """
        self.query.append({"map" : func})
        return self

    def filter(self, func):
        """
        Filter results by the given javascript function.
        function(el) bool
        """
        self.query.append({"filter" : func})
        return self

    def fold(self, init, func):
        self.query.append({"fold": {"init" : init, "source" : func}})
        return self

    def vertexFromValues(self, func):
        """
        """
        self.query.append({"vertex_from_values" : func})
        return self

    def match(self, queries):
        """
        Intersect multiple queries.
        """
        mq = []
        for i in queries:
            mq.append( {'query': i.query} )
        self.query.append({'match': {'queries': mq }})
        return self

    def render(self):
        """
        Return the query as a JSON string.
        """
        output = {'query': self.query}
        return json.dumps(output)

    def __iter__(self):
        return self.execute()

    def execute(self):
        """
        Execute the query and return an iterator.
        """
        payload = self.render()
        #print payload
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        #out = []
        for result in response:
            try:
                d = json.loads(result)
                if 'value' in d:
                    #out.append(d['value'])
                    yield d['value']
                elif 'row' in d:
                    #out.append(d['row'])
                    yield d['row']
            except ValueError, e:
                print "Can't decode: %s" % result
                raise e
        #return out


    def first(self):
        """
        Return only the first result.
        """
        return list(self.execute())[0]
