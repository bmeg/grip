---
title: er

menu:
  main:
    parent: commands
    weight: 6
---

```
grip er
```

The *External Resource* system allows GRIP to plug into existing data systems and
integrate them into queriable graphs. The `grip er` sub command acts as a client
to the external resource plugin proxies, issues command and displays the results.
This is often useful for debugging external resources before making them part of
an actual graph.


List collections provided by external resource
```
grip er list
```

Get info about a collection
```
grip er info
```

List ids from a collection
```
grip er ids
```

List rows from a collection
```
grip er rows
```

List rows with field match
```
grip get
```

List rows with field match
```
grip er query
```
