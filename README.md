# The Arachne Graph Database server

To Install
----------
```
go get github.com/bmeg/arachne
```
If you have defined `$GOPATH` the application will be installed at
`$GOPATH`/bin/arachne otherwise it will be `$HOME/go/bin/arachne`

To Turn on server
-----------------
```
arachne server
```

To Run Larger 'Amazon Data Test'
--------------------------------

Turn on local arachne server


Download test data
```
curl -O http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz
```

Convert the data
```
python $GOPATH/src/github.com/bmeg/arachne/example/amazon_convert.py amazon-meta.txt.gz test.data
```

Create Amazon Graph
```
arachne create amazon
```

List the Graphs
```
arachne list
```

Load data
```
arachne load --edge test.data.edge --vertex test.data.vertex --graph amazon
```

Example queries:
Command line
```
arachne query amazon 'O.query().V().groupCount("group")'
```

Python
```
import aql
import json

conn = aql.Connection("http://localhost:8201")

O = conn.graph("amazon")

#Count the Vertices
print list(O.query().V().count())
#Count the Edges
print list(O.query().E().count())

#Try simple traveral
print list(O.query().V("B00000I06U").outEdge())


#Do a group count of the different 'group's in the graph
print list(O.query().V().groupCount("group"))

#use graph to find every Book that is similar to a DVD
for a in O.query().V().has("group", "Book").mark("a").outgoing("similar").has("group", "DVD").mark("b").select(["a", "b"]):
    print a
```

Matrix Data Loading Example
---------------------------

Create the graph
```
arachne create test-data
```

Add aql.py Python Library to PYTHONPATH
```
export PYTHONPATH=`pwd`
```

Install Pandas if you don't already have it
```
pip install pandas
```

Load Pathway information
```
curl -O http://www.pathwaycommons.org/archives/PC2/v9/PathwayCommons9.All.hgnc.sif.gz
gunzip PathwayCommons9.All.hgnc.sif.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_sif.py PathwayCommons9.All.hgnc.sif
```

Load Matrix data
```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/HiSeqV2.gz
gunzip HiSeqV2.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_matrix.py HiSeqV2
```

Load clinical information
```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/BRCA_clinicalMatrix.gz
gunzip BRCA_clinicalMatrix.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_property_matrix.py BRCA_clinicalMatrix
```

Python Query: Open Connection
```
import aql
conn = aql.Connection("http://localhost:8201")
O = conn.graph("test-data")
```

Print out expression data of all Stage IIA samples
```
for row in O.query().V().hasLabel("Sample").has("pathologic_stage", "Stage IIA").outgoing("has").hasLabel("Data:expression").outgoingBundle("value"):
  print row
```

GraphQL Endpoint
---------------
Arachne supports GraphQL access of the property graphs. Currently this is read-only
access to the graph.
GraphQL graphs have a defined schema with typed fields and connections. This
schema must be defined before the graphql endpoint can access the graph.

All of the different label types in the graph are represented with a vertex of
label 'Object'. The vertex `gid` in the schema graph represents the label type
in the actual graph. Attached to each `Object` vertex is a `fields` parameter
that describes the fields and their data types.

Example Object Vertex:
```
gid: Human
label: Object
data:
  fields:
    name: String
    height: Float
    mass: Float
    homePlanet: String
```

A valid vertex this schema would map to would be:
```
gid: Luke Skywalker
label: Human
data:
  name: Luke Skywalker
  height: 1.72
  mass: 77
  homePlanet: Tatooine
```

Complex Types are described using data the schema data structures with the final
value element being a string on the data type. So an array of strings would be
coded in JSON as `["String"]`. A map of values would be
`["name" : "String", "values" : [float]]`

There is one vertex, of label `Query` that defines the root query element.
There should be one and only one declared in the schema graph. It's `gid` doesn't
matter.

Fields in objects that connect to other nodes can be defined on Object to another
with edge label `field`. The `data` for the edge needs a `name` field to declare
the field name. An optional `label` field can also be added to specify which
edge labels are followed for the field. The field will be projected as an array
of the destination object type.

To connect the `Human` object to its friends:
```
label: field
from: Human
to: Human
data:
  name: friends
  label: friend
```

## Loading the Schema

The example data would be in a file called `data.yaml`:
```
vertices:
  - gid: 1000
    label: Human
    data:
      name: Luke Skywalker
      height: 1.72
      mass: 77
      homePlanet: Tatooine
  - gid: 1001
    label: Human
    data:
      name: Darth Vader
      height: 2.02
      mass: 136
      homePlanet: Tatooine
  - gid: 1002
    label: Human
    data
      name: Han Solo
      height: 1.8
      mass: 80
  - gid: 1003
    label: Human
    data:
      name: Leia Organa
      height: 1.5
      mass: 49
      homePlanet: Alderaan
  - gid: 1004
    label: Human
    data
      name: Wilhuff Tarkin
      height: 1.8
      mass:   nil
edges:
  - {label: "friend", from: "1000", to: "1002"}
  - {label: "friend", from: "1000", to: "1003"}
  - {label: "friend", from: "1001", to: "1004"}
  - {label: "friend", from: "1002", to: "1000"}
  - {label: "friend", from: "1002", to: "1003"}
  - {label: "friend", from: "1003", to: "1000"}
  - {label: "friend", from: "1003", to: "1002"}
  - {label: "friend", from: "1004", to: "1001"}
```

For the friend network, the schema would be a file named `schema.yaml` with:
```
vertices:
  - gid: root
    label: Query
  - gid: Human
    label: Object
    data:
      fields:
        name: String
        height: Float
        mass: Float
        homePlanet: String
edges:
    - label: field
      from: root
      to: Human
      data:
        type: idQuery
        name: Human
    - label: field
      from: Human
      to: Human
      data:
        name: friends
        label : "friend
    - label: field
      from: root
      to: Human
      data:
        name: HumanIds
        type: idList
```

To load the test data:
```
arachne load --graph test --yaml data.yaml
arachne load --graph test:schema --yaml schema.yaml
```


## Using Built in example

Loading the example data and the example schema:
```
./bin/arachne example
```

See the example graph
```
./bin/arachne dump --vertex --edge --graph example
```
See the example graph schema
```
./bin/arachne dump --vertex --edge --graph example:schema
```

## Example GraphQL queries

Get Types:
```
curl -XPOST -H "Content-Type:application/graphql" -d '{__schema{types{name}}}' http://localhost:8201/graphql/example
```

Get Info about Human object
```
curl -XPOST -H "Content-Type:application/graphql" -d '{__type(name:"Human"){fields{name}}}' http://localhost:8201/graphql/example
```

Get List of all Human ids
```
curl -XPOST -H "Content-Type:application/graphql" -d 'query { HumanIds }' http://localhost:8201/graphql/example
```

Get Human 1000 and list their friends
```
curl -XPOST -H "Content-Type:application/graphql" -d 'query {Human(id:"1000"){name,friends{name}}}' http://localhost:8201/graphql/example
```
