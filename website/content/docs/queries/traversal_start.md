
---
title: Start a Traversal
menu:
  main:
    parent: Queries
    weight: 1
---

# Start a Traversal

All traversal based queries must start with a `V()` command, starting the travalers on the vertices of the graph.

## .V([ids])
Start query from Vertex

```python
G.query().V()
```

Returns all vertices in graph

```python
G.query().V(["vertex1"])
```

Returns:
```json
{"_id" : "vertex1", "_label":"TestVertex"}
```

---
## .E([ids])
It is also possible to start queries on the edges of a graph. This method is less often used and may be depricated in the future
Start query from Edge

```python
G.query().E()
```
Returns all edges in graph

```python
G.query().E(["edge1"])
```
Returns:
```json
{"_id" : "edge1", "_label":"TestEdge", "_from": "vertex1", "_to": "vertex2"}
```
