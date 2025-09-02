---
title: Traverse the Graph
menu:
  main:
    parent: Queries
    weight: 3
---

# Traverse the graph
To move travelers between different elements of the graph, the traversal commands `in_` and `out` move along the edges, respecting the directionality. The `out` commands follow `_from` to `_to`, while the `in_` command follows `_to` to `_from`.

## .in_(), inV()
Following incoming edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all incoming edges.

```python
G.query().V().in_(label=['edgeLabel1', 'edgeLabel2'])
```
---

## .out(), .outV()
Following outgoing edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all outgoing edges.

```python
G.query().V().out(label='edgeLabel')
```
---

## .both(), .bothV()
Following all edges (both in and out). Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all edges.

```python
G.query().V().outE().both(label='edgeLabel')
```
---

## .inE()
Following incoming edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

```python
G.query().V().inE(label='edgeLabel')
```
---

## .outE()
Following outgoing edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

```python
G.query().V().outE(label='edgeLabel')
```
---

## .bothE()
Following all edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

```python
G.query().V().bothE(label='edgeLabel')
```
---

# AS and SELECT

The `as_` and `select` commands allow a traveler to mark a step in the traversal and return to it as a later step.

## .as_(name)
Store current row for future reference

```python
G.query().V().as_("a").out().as_("b")
```

## .select(name)
Move traveler to previously marked position

```python
G.query().V().mark("a").out().mark("b").select("a")
```
