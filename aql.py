import json
import urllib2

class Connection:
    def __init__(self, host, graph):
        self.host = host
        self.url =  "%s/v1/graph/%s" % (host, graph)

    def query(self):
        return Query(self)

    def addVertex(self, id, prop={}):
        payload = json.dumps({
            "vertex" : {
                "gid" : "id",
                "properties" : prop
            }
        })
        #print payload
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

    def fold(self, func):
        self.query.append({"fold" : func})
        return self

    def drop(self):
        self.query.append({"drop" : ''})
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
        out = []
        for result in response.readlines():
            try:
                d = json.loads(result)
                if 'value' in d:
                    out.append(d['value'])
                elif 'row' in d:
                    out.append(d['row'])
            except ValueError, e:
                print "Can't decode: %s" % result
                raise e
        return out


    def first(self):
        return self.execute()[0]
