---
title: Federated Graph

menu:
  main:
    parent: gripper
    weight: 3
---

# GRIPPER federated graph

The federated graph is the union of the subgraphs served by the resource nodes. There is no
mapping file: the nodes define the graph and the central server only needs to know where they
are.

```yaml
Default: swapi
Drivers:
  swapi:
    Gripper:
      Graph: swapi
      # Sources: [Character, Film]   # optional subset; default is every source
Sources:
  Character: localhost:50061
  Film: localhost:50062
  Planet: localhost:50063
```

Source prefixes must not overlap. The graph is read only.

## How queries run

The central server cuts a query into *segments*, runs of steps a node can execute, and steps
it runs itself (`limit`, `count`, `select`, `jump`, `*Null` variants, ...). For a segment it
sends one traveler per current element to the node that owns the element and receives the
results asynchronously. Moves to vertices of other nodes become handoffs that the central
server routes to the owner. For example

```
G.V().hasLabel("Character").out("films").out("planets")
```

opens one stream per node involved no matter how many characters there are.

Steps that look at incoming edges (`in`, `inE`, `both`, `bothE`) also ask every node whose
`edge_links` declare edges toward the vertex's prefix.

## Testing

`make test-conformance-federated` starts one node per SWAPI label plus a central server and runs
the conformance suite against them, so every edge between labels crosses nodes.
