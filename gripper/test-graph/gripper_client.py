#!/usr/bin/env python

import grpc
import argparse
import gripper_pb2
import gripper_pb2_grpc
from google.protobuf import json_format


def list_collections(conn, args):
    res = conn.GetCollections(gripper_pb2.Empty())
    for l in res:
        print(l.name)

def list_ids(conn, args):
    req = gripper_pb2.Collection()
    req.name = args.collection
    res = conn.GetIDs(req)
    for l in res:
        print(l.id)

def get_collection(conn, args):
    req = gripper_pb2.Collection()
    req.name = args.collection
    res = conn.GetCollectionInfo(req)
    print(res)

def genIdReqs(ids, collection):
    for i in ids:
        o = gripper_pb2.RowRequest()
        o.collection = collection
        o.id = i
        yield o

def getby_ids(conn, args):
    for row in conn.GetRowsByID(genIdReqs(args.ids, args.collection)):
        print("%s\t%s" % (row.id, json_format.MessageToDict(row.data)))

def list_rows(conf, args):
    req = gripper_pb2.Collection()
    req.name = args.collection
    res = conn.GetRows(req)
    for l in res:
        print("%s\t%s" % (l.id, json_format.MessageToDict(l.data)))

def search_collection(conf, args):
    req = gripper_pb2.FieldRequest()
    req.collection = args.collection
    req.field = args.field
    req.value = args.value
    res = conn.GetRowsByField(req)
    for l in res:
        print("%s\t%s" % (l.id, json_format.MessageToDict(l.data)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', "--server", default="localhost:50051")

    subcmd = parser.add_subparsers(help='sub-command help')

    list_cmd = subcmd.add_parser("list")
    list_cmd.set_defaults(func=list_collections)

    ids_cmd = subcmd.add_parser("ids")
    ids_cmd.add_argument("collection")
    ids_cmd.set_defaults(func=list_ids)

    get_cmd = subcmd.add_parser("get")
    get_cmd.add_argument("collection")
    get_cmd.add_argument("ids", nargs="+")
    get_cmd.set_defaults(func=getby_ids)

    row_cmd = subcmd.add_parser("rows")
    row_cmd.add_argument("collection")
    row_cmd.set_defaults(func=list_rows)

    info_cmd = subcmd.add_parser("info")
    info_cmd.add_argument("collection")
    info_cmd.set_defaults(func=get_collection)

    search_cmd = subcmd.add_parser("search")
    search_cmd.add_argument("collection")
    search_cmd.add_argument("field")
    search_cmd.add_argument("value")
    search_cmd.set_defaults(func=search_collection)

    args = parser.parse_args()

    with grpc.insecure_channel(args.server) as channel:
        conn = gripper_pb2_grpc.DigSourceStub(channel)
        args.func(conn, args)
