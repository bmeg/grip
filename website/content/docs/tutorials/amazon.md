---
title: Amazon Purchase Network

menu:
  main:
    parent: Tutorials
    weight: 1
---

# Explore Amazon Product Co-Purchasing Network Metadata

Download the data

```
curl -O http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz
```

Convert the data into vertices and edges

```
python $GOPATH/src/github.com/bmeg/grip/example/amazon_convert.py amazon-meta.txt.gz amazon.data
```

Create a graph called 'amazon'

```
grip create amazon
```

Load the vertices/edges into the graph

```
grip load amazon --edge amazon.data.edge --vertex amazon.data.vertex
```

Query the graph

_command line client_

```
grip query amazon 'O.query().V().out()'
```

_python client_

```
pip install "git+https://github.com/bmeg/grip.git#egg=gripql&subdirectory=gripql/python/"
```

```python
import gripql

conn = gripql.Connection("http://localhost:8201")

g = conn.graph("amazon")

# Count the Vertices
print g.query().V().count().execute()
# Count the Edges
print g.query().E().count().execute()

# Try simple travesral
print g.query().V("B00000I06U").outE().execute()

# Find every Book that is similar to a DVD
for result in g.query().V().has(gripql.eq("group", "Book")).as_("a").out("similar").has(gripql.eq("group", "DVD")).as_("b").select(["a", "b"]):
    print result
```
