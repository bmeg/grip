---
title: Aggregation
menu:
  main:
    parent: Queries
    weight: 6
---

# Aggregation

These methods provide a powerful way to analyze and summarize data in your GripQL graph database. They allow you to perform various types of aggregations, including term frequency, histograms, percentiles, and more. By combining these with other traversal functions like `has`, `hasLabel`, etc., you can create complex queries that extract specific insights from your data.

## `.aggregate([aggregations])`
Groups and summarizes data from the graph. It allows you to perform calculations on vertex or edge properties. The following aggregation types are available:

## Aggregation Types
### `.gripql.term(name, field, size)`
Return top n terms and their counts for a field.
```python
G.V().hasLabel("Person").aggregate(gripql.term("top-names", "name", 10))
```
Counts `name` occurrences across `Person` vertices and returns the 10 most frequent `name` values.

### `.gripql.histogram(name, field, interval)`
Return binned counts for a field.
```python
G.V().hasLabel("Person").aggregate(gripql.histogram("age-hist", "age", 5))
```
Creates a histogram of `age` values with bins of width 5 across `Person` vertices.

### `.gripql.percentile(name, field, percents=[])`
Return percentiles for a field.
```python
G.V().hasLabel("Person").aggregate(gripql.percentile("age-percentiles", "age", [25, 50, 75]))
```
Calculates the 25th, 50th, and 75th percentiles for `age` values across `Person` vertices.

### `.gripql.field("fields", "$")`
Returns all of the fields found in the data structure. Use `$` to get a listing of all fields found at the root level of the `data` property of vertices or edges.

---

## `.count()`
Returns the total number of elements in the traversal.
```python
G.V().hasLabel("Person").count()
```
This query returns the total number of vertices with the label "Person".

---

## `.distinct([fields])`
Filters the traversal to return only unique elements. If `fields` are provided, uniqueness is determined by the combination of values in those fields; otherwise, the `_id` is used.
```python
G.V().hasLabel("Person").distinct(["name", "age"])
```
This query returns only unique "Person" vertices, where uniqueness is determined by the combination of "name" and "age" values.

---

## `.sort([fields])`
Sort the output using the field values. You can sort in ascending or descending order by providing `descending=True` as an argument to `sort()` method.
```python
G.V().hasLabel("Person").sort("age")
```
This query sorts "Person" vertices based on their age in ascending order.

## `.limit(n)`
Limits the number of results returned by your query.
```python
G.V().hasLabel("Person").limit(10)
```
This query limits the results to the first 10 "Person" vertices found.

---

## `.skip(n)`
Offsets the results returned by your query.
```python
G.V().hasLabel("Person").skip(5)
```
This query skips the first 5 "Person" vertices and returns the rest.


