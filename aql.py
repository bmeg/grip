import json
import urllib2

class Connection:
    def __init__(self, host):
        self.host = host
        self.url =  "%s/v1/graph" % (host)

    def list(self):
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
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request("%s/%s" % (self.url, name), "{}", headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def delete(self, name):
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request("%s/%s" % (self.url, name), headers=headers)
        request.get_method = lambda: "DELETE"
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def graph(self, name):
        return Graph(self.url, name)


class Graph:
    def __init__(self, url, name):
        self.url = url
        self.name = name

    def query(self):
        return Query(self)

    def addVertex(self, id, label, prop={}):
        payload = json.dumps({
            "gid" : id,
            "label" : label,
            "data" : prop
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/vertex", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addEdge(self, src, dst, label, prop={}):
        payload = json.dumps({
            "from" : src,
            "to" : dst,
            "label" : label,
            "data" : prop
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/" + self.name + "/edge", payload, headers=headers)
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

    def mark(self, name):
        q = self.query()
        q.mark(name)
        return q

class BulkAdd:
    def __init__(self, url, graph):
        self.url = url
        self.graph = graph
        self.elements = []

    def addVertex(self, id, label, prop={}):
        payload = json.dumps({
            "graph" : self.graph,
            "vertex" : {
                "gid" : id,
                "label" : label,
                "data" : prop
            }
        })
        self.elements.append(payload)

    def addEdge(self, src, dst, label, prop={}):
        payload = json.dumps({
            "graph" : self.graph,
            "edge" : {
                "from" : src,
                "to" : dst,
                "label" : label,
                "data" : prop
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

class Query:
    def __init__(self, parent=None):
        self.query = []
        self.parent = parent

    def js_import(self, src):
        self.query.append({"import":src})
        return self

    def V(self, id=[]):
        if not isinstance(id, list):
            id = [id]
        self.query.append({"V":id})
        return self

    def E(self, id=None):
        self.query.append({"E":id})
        return self

    def hasLabel(self, label):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'hasLabel': label})
        return self

    def hasId(self, id):
        if not isinstance(id, list):
            id = [id]
        self.query.append({'hasId': id})
        return self

    def has(self, prop, within):
        if not isinstance(within, list):
            within = [within]
        self.query.append({'has': { "key" : prop, 'within': within}})
        return self

    def values(self, v):
        if not isinstance(v, list):
            v = [v]
        self.query.append({'values': {"labels" : v}})
        return self

    def cap(self, c):
        if not isinstance(c, list):
            c = [c]
        self.query.append({'cap': c})
        return self

    def incoming(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'in': label})
        return self

    def outgoing(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'out': label})
        return self

    def both(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'both': label})
        return self

    def incomingEdge(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'inEdge': label})
        return self

    def outgoingEdge(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'outEdge': label})
        return self

    def bothEdge(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'bothEdge': label})
        return self

    def outgoingBundle(self, label=[]):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'outBundle': label})
        return self

    def mark(self, label):
        self.query.append({'as': label})
        return self

    def select(self, labels):
        self.query.append({'select': {"labels" : labels}})
        return self

    def limit(self, l):
        self.query.append({'limit': l})
        return self

    def range(self, begin, end):
        self.query.append({'begin': begin, 'end': end})
        return self

    def count(self):
        self.query.append({'count': ''})
        return self

    def groupCount(self, label):
        self.query.append({'groupCount': label})
        return self

    def by(self, label):
        self.query.append({'by': label})
        return self

    def map(self, func):
        self.query.append({"map" : func})
        return self

    def filter(self, func):
        self.query.append({"filter" : func})
        return self

    def fold(self, func):
        self.query.append({"fold" : func})
        return self

    def vertexFromValues(self, func):
        self.query.append({"vertexFromValues" : func})
        return self

    def match(self, queries):
        mq = []
        for i in queries:
            mq.append( {'query': i.query} )
        self.query.append({'match': {'queries': mq }})
        return self

    def render(self):
        output = {'query': self.query}
        return json.dumps(output)

    def __iter__(self):
        return self.execute()

    def execute(self):
        payload = self.render()
        #print payload
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = self.parent.url + "/" + self.parent.name + "/query"
        request = urllib2.Request(url, payload, headers=headers)
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
        return list(self.execute())[0]
