
---
title: Start a Traversal
menu:
  main:
    parent: Queries
    weight: 1
---

# Start a Traversal

All traversal based queries must start with a `V()` command, starting the travalers on the vertices of the graph.

## `.V([ids])`
Start query from Vertex

```python
G.V()
```

Returns all vertices in graph

```python
G.V(["vertex1"])
```

Returns:
```json
{"_id" : "vertex1", "_label":"TestVertex"}
```
