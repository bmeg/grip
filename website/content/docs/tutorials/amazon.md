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

Turn on grip and create a graph called 'amazon'

```
grip server & ; sleep 1 ; grip create amazon
```

Load the vertices/edges into the graph

```
grip load amazon --edge amazon.data.edge --vertex amazon.data.vertex
```

Query the graph

_command line client_

```
grip query amazon 'V().hasLabel("Video").out()'
```

The full command syntax and command list can be found at grip/gripql/javascript/gripql.js

_python client_

Initialize a virtual environment and install gripql python package

```
python -m venv venv ; source venv/bin/activate
pip install -e gripql/python
```

Example code

```python
import gripql

conn = gripql.Connection("http://localhost:8201")

g = conn.graph("amazon")

# Count the Vertices
print("Total vertices: ", g.V().count().execute())
# Count the Edges
print("Total edges: ", g.V().outE().count().execute())

# Try simple travesral
print("Edges connected to 'B00000I06U' vertex: %s" %g.V("B00000I06U").outE().execute())

# Find every Book that is similar to a DVD
for result in g.V().has(gripql.eq("group", "Book")).as_("a").out("similar").has(gripql.eq("group", "DVD")).as_("b").select("a"):
    print(result)
```
