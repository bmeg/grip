from __future__ import absolute_import, print_function, unicode_literals

import json
import urllib2

from aql.util import do_request
from aql.graph import Graph


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
        response = do_request(request)
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
        response = do_request(request)
        result = response.read()
        return json.loads(result)

    def graph(self, name):
        """
        Get a graph handle.
        """
        return Graph(self.url, name)
