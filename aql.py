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
        return Graph("%s/%s" % (self.url, name))


class Graph:
    def __init__(self, url):
        self.url = url

    def query(self):
        return Query(self)

    def addVertex(self, id, prop={}):
        payload = json.dumps({
            "gid" : id,
            "properties" : prop
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/vertex", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def addEdge(self, src, dst, label, prop={}):
        payload = json.dumps({
            "src" : src,
            "dst" : dst,
            "label" : label,
            "properties" : prop
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/edge", payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)


    def addBundle(self, src, bundle, label):
        payload = json.dumps({
            "src" : src,
            "bundle" : bundle,
            "label" : label,
        })
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url + "/bundle", payload, headers=headers)
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

    def V(self, id=None):
        self.query.append({"V":id})
        return self

    def E(self, id=None):
        self.query.append({"E":id})
        return self

    def label(self, label):
        self.query.append({'label': label})
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

    def incoming(self, label=""):
        self.query.append({'in': label})
        return self

    def outgoing(self, label=""):
        self.query.append({'out': label})
        return self

    def inEdge(self, label=""):
        self.query.append({'inEdge': label})
        return self

    def outEdge(self, label=""):
        self.query.append({'outEdge': label})
        return self

    def inVertex(self, label):
        self.query.append({'inVertex': label})
        return self

    def outVertex(self, label):
        self.query.append({'outVertex': label})
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

    def render(self):
        output = {'query': self.query}
        return json.dumps(output)

    def execute(self):
        payload = self.render()
        #print payload
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.parent.url + "/query", payload, headers=headers)
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
                #print "Can't decode: %s" % result
                raise e
        #return out


    def first(self):
        return list(self.execute())[0]
