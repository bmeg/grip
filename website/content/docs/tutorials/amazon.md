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

O = conn.graph("amazon")

# Count the Vertices
print O.query().V().count().execute()
# Count the Edges
print O.query().E().count().execute()

# Try simple travesral
print O.query().V("B00000I06U").outEdge().execute()

# Find every Book that is similar to a DVD
for result in O.query().V().where(gripql.eq("group", "Book")).mark("a").out("similar").where(gripql.eq("group", "DVD")).mark("b").select(["a", "b"]):
    print result
```
