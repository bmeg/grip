---
title: Operations
menu:
  main:
    parent: Queries
    weight: -5
---


# Start a Traversal
## .V([ids])
Start query from Vertex

```python
O.query().V()
```
Returns all vertices in graph

```python
O.query().V(["vertex1]")
```
Returns:
```json
{"gid" : "vertex1", "label":"TestVertex", "data":{}}
```

## .E([ids])
Start query from Edge

```python
O.query().E()
```
Returns all edges in graph

```python
O.query().E(["edge1"])
```
Returns:
```json
{"gid" : "edge1", "label":"TestEdge", From: "vertex1", To: "vertex2", data":{}}
```


# Traverse the graph
## .in_()
Following incoming edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all incoming edges.

## .out()
Following outgoing edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all outgoing edges.

## .both()
Following all edges (both in and out). Optional argument is the edge label (or list of labels) that should be followed.

## .inEdge()
Following incoming edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

## .outEdge()
Following outgoing edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

## .bothEdge()
Following all edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`


# Filtering
## .where()
Filter elements using conditional statements

```python
O.query().V().where(gripql.eq("_label", "Gene")).where(gripql.eq("symbol", "TP53"))
```

## Conditions
Conditions are arguments to `.where()` that define selection conditions

### gripql.eq(variable, value)
Returns rows where variable == value
```python
.where(gripql.eq("symbol", "TP53"))
```

### gripql.neq(variable, value)
Returns rows where variable != value
```python
.where(gripql.neq("symbol", "TP53"))
```

### gripql.gt(variable, value)
Returns rows where variable > value
```python
.where(gripql.gt("age", 45))
```

### gripql.lt(variable, value)
Returns rows where variable < value
```python
.where(gripql.lt("age", 45))
```

### gripql.gte(variable, value)
Returns rows where variable >= value
```python
.where(gripql.gte("age", 45))
```

### gripql.lte(variable, value)
Returns rows where variable <= value
```python
.where(gripql.lte("age", 45))
```

### gripql.in_(variable, value)
Returns rows where variable in value
```python
.where(gripql.in_("symbol", ["TP53", "BRCA1"]))
```

### gripql.contains(variable, value)
Returns rows where variable contains value
```python
.where(gripql.in_("groups", "group1"))
```

Returns:
```
{"data" : {"groups" : ["group1", "group2"]}}
```

### gripql.and_([conditions])
```python
.where(gripql.and_( [gripql.lte("age", 45), gripql.gte("age", 35)] ))
```

### gripql.or_([conditions])
```python
.where(gripql.or_( [...] ))
```

### gripql.not_(condition)
```python
.where(gripql.not_( [...] ))
```

# Output
## .mark(name)
Store current row for future reference
```python
O.query().V().mark("a").out().mark("b")
```

## .select([names])
Output previously marked elements
```python
O.query().V().mark("a").out().mark("b").select(["a", "b"])
```

## .limit(count)
Limit number of total output rows
```python
O.query().V().limit(5)
```

## .offset(count)
Start return after offset
```python
O.query().V().offset(5).limit(5)
```

## .fields([fields])
Select which vertex/edge fields to return
```python
O.query().V("vertex1").fields("_gid", "_label", "symbol")
```

## .render(template)
Render current selection into arbitrary data structure
```python
O.query().V("vertex1").render()
```

## .aggregate([aggregations])
Aggregate fields in the returned edges/vertices.

## Aggregation Types
### .gripql.term(name, label, field, size)
Return top n terms and their counts for a field.
```
O.query().V("1").out().aggregate(gripql.histogram("top-names", "Person", "name", 10))
```
Starts on vertex `1`, goes out and then counts `name` occurences across `Person` vertices and returns the 10 most frequent `name` values.


### .gripql.histogram(name, label, field, interval)
Return binned counts for a field.
```
O.query().V("1").out().aggregate(gripql.histogram("age-hist", "Person", "age", 5))
```
Starts on vertex `1`, goes out and then creates a histogram of `age` values with bins of width 5 across `Person` vertices.


### .gripql.percentile(name, label, field, percents=[])
Return percentiles for a field.
```
O.query().V("1").out().aggregate(gripql.percentile("age-hist", "Person", "age", [25,50,75]))
```
Starts on vertex `1`, goes out and then calculates the 25th, 50th, and 75th percentiles for `age` values across `Person` vertices.

## .count()
Return the total count of returned edges/vertices.

## .distinct([fields])
Only return distinct elements. An array of one or more fields may be passed in to define what elements are used to identify uniqueness. If none are
provided, the `gid` is used.
