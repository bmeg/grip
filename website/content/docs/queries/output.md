---
title: Output Control
menu:
  main:
    parent: Queries
    weight: 10
---

---

# Output control

## `.limit(count)`
Limit number of total output rows
```python
G.V().limit(5)
```
---
## `.skip(count)`
Start return after offset

Example:
```python
G.V().skip(10).limit(5)

```
This query skips the first 10 vertices and then returns the next 5.
---
## `.range(start, stop)`
Selects a subset of the results based on their index.  `start` is inclusive, and `stop` is exclusive.
Example:
```python
G.V().range(5, 10)
```
---
## `.fields([fields])`
Specifies which fields of a vertex or edge to include or exclude in the output. By default, `_id`, `_label`, `_from`, and `_to` are included.

If `fields` is empty, all properties are excluded.
If `fields` contains field names, only those properties are included.
If `fields` contains field names prefixed with `-`, those properties are excluded, and all others are included.

Examples:

Include only the 'symbol' property:
```python
G.V("vertex1").fields(["symbol"])
```

Exclude the 'symbol' property:
```python
G.V("vertex1").fields(["-symbol"])
```
Exclude all properties:
```python
G.V("vertex1").fields([])
```

---

## `.render(template)`

Transforms the current selection into an arbitrary data structure defined by the `template`. The `template` is a string that can include placeholders for vertex/edge properties.

Example:
```python
G.V("vertex1").render( {"node_info" : {"id": "$._id", "label": "$._label"}, "data" : {"whatToExpect": "$.climate"}} )
```

Assuming `vertex1` has `_id`, `_label`, and `symbol` properties, this would return a JSON object with those fields.

```json
{"node_info" : {"id" :"Planet:2", "label":"Planet"}, "data":{"whatToExpect":"arid"} }
```
