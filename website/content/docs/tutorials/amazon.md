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
python $GOPATH/src/github.com/bmeg/arachne/example/amazon_convert.py amazon-meta.txt.gz amazon.data
```

Create a graph called 'amazon'

```
arachne create amazon
```

Load the vertices/edges into the graph

```
arachne load amazon --edge amazon.data.edge --vertex amazon.data.vertex
```

Query the graph

_command line client_

```
arachne query amazon 'O.query().V().out()'
```

_python client_

```
pip install "git+https://github.com/bmeg/arachne.git#egg=aql&subdirectory=aql/python/"
```

```python
import aql

conn = aql.Connection("http://localhost:8201")

O = conn.graph("amazon")

# Count the Vertices
print O.query().V().count().execute()
# Count the Edges
print O.query().E().count().execute()

# Try simple travesral
print O.query().V("B00000I06U").outEdge().execute()

# Find every Book that is similar to a DVD
for result in O.query().V().where(aql.eq("group", "Book")).mark("a").out("similar").where(aql.eq("group", "DVD")).mark("b").select(["a", "b"]):
    print result
```
