---
title: Intro

menu:
  main:
    parent: gripper
    weight: 1
---

# GRIPPER
## GRIP Plugin External Resources

GRIP Plugin External Resources (GRIPPERs) are GRIP drivers that take external
resources and allow GRIP to access them are part of a unified graph.
To integrate new resources into the graph, you
first deploy griper proxies that plug into the external resources. They are unique
and configured to access specific resources. These provide a view into external
resources as a series of document collections. For example, an SQL gripper would
plug into an SQL server and provide the tables as a set of collections with each
every row a document. A gripper is written as a gRPC server.

![GIPPER Architecture](/img/gripper_architecture.png)
