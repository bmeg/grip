

from __future__ import print_function
from ctypes import *
from ctypes.util import find_library
import os, inspect, sysconfig
import random, string
import json
from gripql.query import QueryBuilder

cwd = os.getcwd()
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
print("cd to %s" % (currentdir))
os.chdir(currentdir)
_lib = cdll.LoadLibrary("./_pygrip" + sysconfig.get_config_vars()["EXT_SUFFIX"])
os.chdir(cwd)

_lib.ReaderNext.restype = c_char_p

class GoString(Structure):
    _fields_ = [("p", c_char_p), ("n", c_longlong)]

def NewMemServer():
    print(dir(_lib))
    return GraphDBWrapper( _lib.NewMemServer() )

def getGoString(s):
    return GoString(bytes(s, encoding="raw_unicode_escape"), len(s))

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

class QueryWrapper(QueryBuilder):
    def __init__(self, wrapper):
        super(QueryBuilder, self).__init__()
        self.query = []
        self.wrapper = wrapper

    def __iter__(self):
        jquery = json.dumps({ "graph" : "default", "query" : self.query })
        reader = _lib.Query( self.wrapper._handle, getGoString(jquery) )
        while not _lib.ReaderDone(reader):
            j = _lib.ReaderNext(reader)
            yield json.loads(j)

    def _builder(self):
        return QueryWrapper(self.wrapper)

class GraphDBWrapper:
    def __init__(self, handle) -> None:
        self._handle = handle

    def addVertex(self, gid, label, data={}):
        """
        Add vertex to a graph.
        """
        _lib.AddVertex(self._handle, getGoString(gid), getGoString(label), getGoString(json.dumps(data)))
    
    def addEdge(self, src, dst, label, data={}, gid=None):
        """
        Add edge to a graph.
        """
        if gid is None:
            gid = id_generator(10)

        _lib.AddEdge(self._handle, getGoString(gid), 
                     getGoString(src), getGoString(dst), getGoString(label), 
                     getGoString(json.dumps(data)))
        


    def V(self, *ids):
        return QueryWrapper(self).V(*ids)