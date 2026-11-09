---
title: Intro

menu:
  main:
    parent: gripper
    weight: 1
---

# GRIPPER
## GRIP Plugin External Resources

GRIP Plugin External Resources (GRIPPERs) let one GRIP server query several independent
graph databases as a single **federated graph**. Each database is served by a *resource
node* that implements the `GRIPSource` gRPC API. The central GRIP server splits each query
into pieces that the nodes can run themselves, streams travelers to them and merges what
comes back.

![GIPPER Architecture](/img/gripper_architecture.png)

Every resource node serves a subgraph in which all vertex and edge IDs start with the same
CURIE prefix, such as `Character:`. Edges stored in one node may point at vertices owned by
another node.

GRIPPER version 2 is a breaking change. The collection based API, the Python proxies and the
vertex/edge mapping files of version 1 are gone; resource nodes are now written in Go on top
of the library in `gripper/node`.
