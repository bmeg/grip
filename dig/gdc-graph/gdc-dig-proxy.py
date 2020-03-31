#!/usr/bin/env python

import os
import sys
import json
import grpc
import digdriver_pb2
import digdriver_pb2_grpc

from google.protobuf import json_format
from concurrent import futures


class GDCSource(digdriver_pb2_grpc.DigSourceServicer):
    def __init__(self):
        pass

    def GetCollections(self, request, context):
        for i in []:
            o = digdriver_pb2.Collection()
            o.name = i
            yield o

    def GetCollectionInfo(self, request, context):
        o = digdriver_pb2.CollectionInfo()
        # request.name
        return o

    def GetIDs(self, request, context):
        # request.name
        for k in []:
            o = digdriver_pb2.RowID()
            o.id = k
            yield o

    def GetRows(self, request, context):
        # request.name
        for k,v in {}.items():
            o = digdriver_pb2.Row()
            o.id = k
            json_format.ParseDict(v, o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            d = self.data[req.collection][req.id]
            o = digdriver_pb2.Row()
            o.id = req.id
            o.requestID = req.requestID
            json_format.ParseDict(d, o.data)
            yield o

    def GetRowsByField(self, request, context):
        c = self.data[request.collection]
        for k, v in c.items():
            if v.get(request.field, None) == request.value:
                o = digdriver_pb2.Row()
                o.id = k
                json_format.ParseDict(v, o.data)
                yield o


def serve(port):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  digdriver_pb2_grpc.add_DigSourceServicer_to_server(
      GDCSource(), server)
  server.add_insecure_port('[::]:%s' % port)
  server.start()
  print("Serving: %s" % (port))
  server.wait_for_termination()


if __name__ == "__main__":
    serve(50051)
