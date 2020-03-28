#!/usr/bin/env python

import os
import sys
import grpc
import multidriver_pb2
import multidriver_pb2_grpc

from google.protobuf import json_format
from concurrent import futures


class CollectionServicer(multidriver_pb2_grpc.CollectionServicer):
    def __init__(self, data):
        self.data = data

    def GetServiceInfo(self, request, context):
        o = multidriver_pb2.ServiceInfo()
        return o

    def GetIDs(self, request, context):
        print("getids")

        for k in self.data[request.collection]:
            o = multidriver_pb2.RowID()
            o.id = k
            yield o


    def GetRows(self, request, context):
        print("getrows")

        for k,v in self.data[request.collection].items():
            o = multidriver_pb2.Row()
            o.id = k
            json_format.ParseDict(v, o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        print("getrowsbyid")
        # missing associated documentation comment in .proto file
        pass
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def serve(port, data):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  multidriver_pb2_grpc.add_CollectionServicer_to_server(
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
                        d = dict(zip(header,r))
                        data[d['id']] = d
            dataMap[name] = data
    serve(50051, dataMap)
