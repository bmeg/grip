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
G.query().V()
```
Returns all vertices in graph

```python
G.query().V(["vertex1]")
```
Returns:
```json
{"gid" : "vertex1", "label":"TestVertex", "data":{}}
```

## .E([ids])
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
{"gid" : "edge1", "label":"TestEdge", From: "vertex1", To: "vertex2", data":{}}
```


# Traverse the graph
## .in_(), inV()
Following incoming edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all incoming edges.

## .out(), .outV()
Following outgoing edges. Optional argument is the edge label (or list of labels) that should be followed. If no argument is provided, all outgoing edges.

## .both(), .bothV()
Following all edges (both in and out). Optional argument is the edge label (or list of labels) that should be followed.

## .inE()
Following incoming edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

## .outE()
Following outgoing edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`

## .bothE()
Following all edges, but return the edge as the next element. This can be used to inspect edge properties. Optional argument is the edge label (or list of labels) that should be followed. To return back to a vertex, use `.in_` or `.out`


# Filtering
## .has()
Filter elements using conditional statements

```python
G.query().V().has(gripql.eq("_label", "Gene")).has(gripql.eq("symbol", "TP53"))
```

## Conditions
Conditions are arguments to `.has()` that define selection conditions

### gripql.eq(variable, value)
Returns rows where variable == value
```python
.has(gripql.eq("symbol", "TP53"))
```

### gripql.neq(variable, value)
Returns rows where variable != value
```python
.has(gripql.neq("symbol", "TP53"))
```

### gripql.gt(variable, value)
Returns rows where variable > value
```python
.has(gripql.gt("age", 45))
```

### gripql.lt(variable, value)
Returns rows where variable < value
```python
.has(gripql.lt("age", 45))
```

### gripql.gte(variable, value)
Returns rows where variable >= value
```python
.has(gripql.gte("age", 45))
```

### gripql.lte(variable, value)
Returns rows where variable <= value
```python
.has(gripql.lte("age", 45))
```


### gripql.inside(variable, [lower_bound, upper_bound])
Returns rows where variable > lower_bound && variable < upper_bound
```python
.has(gripql.inside("age", [30, 45]))
```


### gripql.outside(variable, [lower_bound, upper_bound])
Returns rows where variable < lower_bound || variable > upper_bound
```python
.has(gripql.outside("age", [30, 45]))
```


### gripql.between(variable, [lower_bound, upper_bound])
Returns rows where variable >= lower_bound && variable < upper_bound
```python
.has(gripql.between("age", [30, 45]))
```


### gripql.within(variable, value)
Returns rows where variable is within provided values
```python
.has(gripql.within("symbol", ["TP53", "BRCA1"]))
```

### gripql.without(variable, value)
Returns rows where variable is not within provided values
```python
.has(gripql.within("symbol", ["TP53", "BRCA1"]))
```

### gripql.contains(variable, value)
Returns rows where variable contains value
```python
.has(gripql.in_("groups", "group1"))
```

Returns:
```
{"data" : {"groups" : ["group1", "group2"]}}
```

### gripql.and_([conditions])
```python
.has(gripql.and_( [gripql.lte("age", 45), gripql.gte("age", 35)] ))
```

### gripql.or_([conditions])
```python
.has(gripql.or_( [...] ))
```

### gripql.not_(condition)
```python
.has(gripql.not_( [...] ))
```

# Output
## .as_(name)
Store current row for future reference
```python
G.query().V().as_("a").out().as_("b")
```

## .select([names])
Output previously marked elements
```python
G.query().V().mark("a").out().mark("b").select(["a", "b"])
```

## .limit(count)
Limit number of total output rows
```python
G.query().V().limit(5)
```

## .skip(count)
Start return after offset
```python
G.query().V().skip(5).limit(5)
```

## .range(start, stop)
As results are iterated return objects starting with lower index As traversers propagate through the traversal, it is possible to only allow a certain number of them to pass through with range()-step (filter). When the low-end of the range is not met, objects are continued to be iterated. When within the low (inclusive) and high (exclusive) range, traversers are emitted. When above the high range, the traversal breaks out of iteration. Finally, the use of -1 on the high range will emit remaining traversers after the low range begins.
```python
G.query().V().range(5, 15)
```


## .fields([fields])
Select which vertex/edge fields to return or exlucde. Operation with no arguments exlcudes all properties. 
"gid", "label", "from" and "to" are included by default.
```python
O.query().V("vertex1").fields("symbol")     # include only symbol property
O.query().V("vertex1").fields("-symbol")    # exclude symbol property
O.query().V("vertex1").fields()             # exclude all properties
```

## .render(template)
Render current selection into arbitrary data structure
```python
G.query().V("vertex1").render()
```

## .aggregate([aggregations])
Aggregate fields in the returned edges/vertices.

## Aggregation Types
### .gripql.term(name, field, size)
Return top n terms and their counts for a field.
```
G.query().V().hasLabel("Person").aggregate(gripql.term("top-names", "name", 10))
```
Counts `name` occurences across `Person` vertices and returns the 10 most frequent `name` values.


### .gripql.histogram(name, field, interval)
Return binned counts for a field.
```
G.query().V().hasLabel("Person").aggregate(gripql.histogram("age-hist", "age", 5))
```
Creates a histogram of `age` values with bins of width 5 across `Person` vertices.


### .gripql.percentile(name, field, percents=[])
Return percentiles for a field.
```
G.query().V().hasLabel("Person").aggregate(gripql.percentile("age-percentiles", "age", [25,50,75]))
```
Calculates the 25th, 50th, and 75th percentiles for `age` values across `Person` vertices.


## .count()
Return the total count of returned edges/vertices.


## .distinct([fields])
Only return distinct elements. An array of one or more fields may be passed in to define what elements are used to identify uniqueness. If none are
provided, the `gid` is used.
