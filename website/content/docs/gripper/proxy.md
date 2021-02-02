---
title: External Resource Proxies

menu:
  main:
    parent: GIPPER
    weight: 2
---

# GRIPPER

## GRIPPER proxy

With the external resources normalized to a single data model, the graph model
describes how to connect the set of collections into a graph model. Each GRIPPER
is required to provide a GRPC interface that allows access to collections stored
in the resource.

The required functions include:

```
rpc GetCollections(Empty) returns (stream Collection);
```
`GetCollections` returns a list of all of the Collections accessible via this server.

```
rpc GetCollectionInfo(Collection) returns (CollectionInfo);
```
`GetCollectionInfo` provides information, such as the list of indexed fields, in a collection.

```
rpc GetIDs(Collection) returns (stream RowID);
```
`GetIDs` returns a stream of all of the IDs found in a collection.

```
rpc GetRows(Collection) returns (stream Row);
```
`GetRows` returns a stream of all of the rows in a collection.

```
rpc GetRowsByID(stream RowRequest) returns (stream Row);
```
`GetRowsByID` accepts a stream of row requests, each one requesting a single row
by it's id, and then returns a stream of results.

```
rpc GetRowsByField(FieldRequest) returns (stream Row);
```
`GetRowsByField` searches a collection, looking for values found in an indexed field.
