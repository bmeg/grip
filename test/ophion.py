import json
import urllib2

class Ophion:
    def __init__(self, host):
        self.host = host
        self.url = host + "/v1/graph-query"

    def query(self):
        return OphionQuery(self)

    def execute(self, query):
        payload = query.render()
        #print payload
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        out = []
        for result in response.readlines():
            try:
                out.append(json.loads(result))
            except ValueError, e:
                print "Can't decode: %s" % result
                raise e
        return out

class OphionQuery:
    def __init__(self, parent=None):
        self.query = []
        self.parent = parent

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
        self.query.append({'values': v})
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

    def inEdge(self, label):
        self.query.append({'inEdge': label})
        return self

    def outEdge(self, label):
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

    def addV(self, id):
        self.query.append({'addV': id})
        return self

    def addE(self, label):
        self.query.append({"addE" : label})
        return self

    def to(self, dst):
        self.query.append({"to" : dst})
        return self

    def property(self, key, value):
        self.query.append({"property" : {"key" : key, "value" : value}})
        return self

    def render(self):
        output = {'query': self.query}
        return json.dumps(output)

    def execute(self):
        return self.parent.execute(self)
