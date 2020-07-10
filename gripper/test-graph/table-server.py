#!/usr/bin/env python

import os
import re
import sys
import json
import grpc
import gripper_pb2
import gripper_pb2_grpc

from google.protobuf import json_format
from concurrent import futures

def keyUnion(a):
    o = set()
    for i in a:
        o.update(*a)
    return list(o)

class CollectionServicer(gripper_pb2_grpc.DigSourceServicer):
    def __init__(self, data):
        self.data = data

    def GetCollections(self, request, context):
        for i in self.data:
            o = gripper_pb2.Collection()
            o.name = i
            yield o

    def GetCollectionInfo(self, request, context):
        o = gripper_pb2.CollectionInfo()
        c = self.data[request.name]
        for f in keyUnion(i.keys() for i in c.values()):
            o.search_fields.append( "$." + f)
        return o

    def GetIDs(self, request, context):
        for k in self.data[request.name]:
            o = gripper_pb2.RowID()
            o.id = k
            yield o

    def GetRows(self, request, context):
        for k,v in self.data[request.name].items():
            o = gripper_pb2.Row()
            o.id = k
            json_format.ParseDict(v, o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            d = self.data[req.collection][req.id]
            o = gripper_pb2.Row()
            o.id = req.id
            o.requestID = req.requestID
            json_format.ParseDict(d, o.data)
            yield o

    def GetRowsByField(self, request, context):
        c = self.data[request.collection]
        f = re.sub( r'^\$\.', '', request.field)
        for k, v in c.items():
            if v.get(f, None) == request.value:
                o = gripper_pb2.Row()
                o.id = k
                json_format.ParseDict(v, o.data)
                yield o


def serve(port, data):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
  gripper_pb2_grpc.add_DigSourceServicer_to_server(
      CollectionServicer(data), server)
  server.add_insecure_port('[::]:%s' % port)
  server.start()
  print("Serving: %s" % (port))
  server.wait_for_termination()


if __name__ == "__main__":
    tableMap = {}
    dataMap = {}
    with open(sys.argv[1]) as handle:
        for line in handle:
            row = line.rstrip().split("\t")
            name = row[0]
            path = os.path.join( os.path.dirname(os.path.abspath(sys.argv[1]) ), row[1] )

            data = {}
            with open(path) as h:
                header = None
                for l in h:
                    r = l.rstrip().split("\t")
                    if header is None:
                        header = r
                    else:
                        j = list(json.loads(i) for i in r)
                        d = dict(zip(header,j))
                        if 'id' in d:
                            data[str(d['id'])] = d
                        else:
                            data[str(len(data))] = d
            dataMap[name] = data
    serve(50051, dataMap)
