---
title: Resource Nodes

menu:
  main:
    parent: gripper
    weight: 2
---

# GRIPPER resource nodes

A resource node is a gRPC server for the `GRIPSource` service:

```
rpc GetInfo(Empty) returns (SourceInfo);
rpc Traverse(stream TraverseRequest) returns (stream TraverseResponse);
```

`GetInfo` returns the node's ID prefix, its schema (vertex and edge labels) and its
`edge_links`: the edges it stores that connect to other prefixes.

`Traverse` runs a fixed *segment* of GripQL steps (`V`, `has*`, `as`, `in/out/both`,
`inE/outE/bothE`). The client first sends a `Start` message with the steps and the cursors
(step indexes) at which travelers may enter. It then streams batches of travelers, each with a
`request_id`, a cursor and the current element. The node answers asynchronously with
batches of results that carry the same `request_id`:

* a result at the end of the segment (`cursor == len(steps)`) with the new current element,
  the marks created and the path elements added inside the segment;
* a *handoff* (`handoff: true`) when a move reached a vertex whose ID has another prefix.
  The central server forwards the traveler to the owner, which loads the vertex and
  continues at the given cursor;
* nothing when the traveler was eliminated.

A `Flush` message is a barrier: the node answers with `FlushAck` once every traveler sent
before it has been processed and all results were sent.

## Writing a node

Use the Go library in `gripper/node` to serve any `gdbi.GraphInterface`:

```go
srv, err := node.NewServer(graph, node.Options{Name: "chars", Prefix: "Character:"})
srv.Register(grpcServer)
```

or run `grip node --prefix Character: --db character.db --vertices ... --edges ...`.
