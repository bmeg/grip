# GRIPPER

GRIP Plugin External Resources (GRIPPERs) let one GRIP server query several independent
graph databases as a single **federated graph**. Each database is served by a *resource
node* that speaks the `GRIPSource` gRPC API. The central GRIP server splits every query
into pieces that the nodes can run themselves, streams travelers to them and merges the
results.

```
                 +----------------------+
 GripQL query -> |  central GRIP server |   plans segments, keeps traveler state,
                 |  (gripper.Federated) |   runs the steps nodes cannot run
                 +----------------------+
                   |        |        |        bidirectional Traverse streams
              +----+   +----+   +----+
              | A: |   | B: |   | C: |        resource nodes, one subgraph each
              +----+   +----+   +----+
```

> GRIPPER v2 replaces the original collection based API (`GetRows`, `GetRowsByField`, ...),
> the Python proxies and the YAML vertex/edge mapping files. There is no compatibility
> with the old interface. See [DESIGN.md](DESIGN.md) for the full design and protocol.

## Concepts

* **Prefix.** Every resource node serves a subgraph in which *all vertex and edge IDs start
  with the same CURIE prefix*, for example `Character:`. Prefixes of the nodes in a
  federation must not overlap.
* **Cross-node edges.** An edge is stored in one node but may point at a vertex that
  belongs to another node, identified by its prefix. Nodes advertise such edges in
  `GetInfo` (`edge_links`) so the central server knows which nodes to ask for incoming edges.
* **Segments.** The central server cuts a query into runs of steps a node can execute
  (`V`, `hasLabel`, `hasId`, `hasKey`, `has`, `as`, `in/out/both`, `inE/outE/bothE`). The
  other steps (`limit`, `count`, `select`, `jump`, `*Null` ...) run on the central server.
* **Travelers.** The central server streams travelers (only the current element) to the
  nodes. The node runs them through the segment and returns, for each, descendants,
  nothing (the traveler was eliminated) or a *handoff* when a move reaches a vertex owned
  by another node. Every result carries the `request_id` of the traveler that was sent.

## Running a federation

Start one node per subgraph. `grip node` serves any GRIP storage driver; the optional
`--vertices`/`--edges` flags load JSON lines files first:

```
grip node --prefix Character: --listen localhost:50061 --db character.db \
          --vertices Character.vertices --edges Character.edges
grip node --prefix Film:      --listen localhost:50062 --db film.db ...
```

Point a server at them. The `Gripper` driver exposes the sources as one graph:

```yaml
Default: swapi
Drivers:
  swapi:
    Gripper:
      Graph: swapi          # name of the federated graph
      # Sources: [Character, Film]   # optional subset of the sources below
Sources:                     # name -> host:port of each resource node
  Character: localhost:50061
  Film: localhost:50062
```

Sources can also be given on the command line with `grip server --er name=host:port`.
Binaries named `gripper-<name>` in the plugin directory (`--plugins`) are launched
and registered as sources through the `StartPlugin` API.

The federated graph is read only. Run `make test-conformance-federated` to start six nodes
(one per SWAPI label, so every edge crosses nodes) and run the conformance suite against them.

`grip node --mapping tables.yaml` loads flat CSV or JSON lines tables into the node's graph
(see [node/table](node/table/table.go) for the mapping format: vertex tables, and edge rows
that join two ID fields). Databases such as SQL or MongoDB are served directly with
`grip node --config` and the matching driver.

## Writing a resource node in Go

Resource nodes are Go programs built on [node](node/):

```go
srv, _ := node.NewServer(graph, node.Options{Name: "chars", Prefix: "Character:"})
srv.Register(grpcServer)
```

`graph` is any `gdbi.GraphInterface`. `node.ScanEdgeLinks` computes the `EdgeLinks`
option from the stored edges. The server compiles each segment with the regular GRIP
query engine over a `BoundaryGraph` that turns moves to foreign vertices into handoffs.

## Semantics worth knowing

* Edges to vertices that no registered node owns, or whose owner does not have the vertex,
  are dropped (with a debug log), like dangling edges in a single graph.
* `in`, `inE`, `both` and `bothE` ask the vertex owner *and* every node whose `edge_links`
  mention the prefix.
* `inNull`/`outNull`/`outENull`/`inENull` run on the central server.
* A node limits segment length and the number of batches it processes concurrently.
* A node or connection failure ends the affected query early and is logged on the central
  server; it is not yet reported to the client as an error (see DESIGN.md, hardening).
