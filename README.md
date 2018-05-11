# The Arachne Graph Database Server

## Installation

```
go get github.com/bmeg/arachne
```

If you have defined `$GOPATH` the application will be installed at
`$GOPATH`/bin/arachne otherwise it will be `$HOME/go/bin/arachne`

## Turning on the Server

```
arachne server
```

### Configuration

Below is the default configuration for Arachne.

```
# The name of the active server database backend
# Available backends: badger, bolt, level, rocks, elastic, mongo
Database: badger
Server:
  HTTPPort: 8201
  RPCPort: 8202
  WorkDir: ./arachne.work
  ContentDir: ""
  ReadOnly: false

# The location where the key-value store should store its data.
# This is used by badger, bolt, level and rocks. 
KVStorePath: ./arachne.db

ElasticSearch:
  URL: ""
  DBName: arachnedb
  Synchronous: false
  BatchSize: 1000

MongoDB:
  URL: ""
  DBName: arachnedb
  BatchSize: 1000
```

## Importing Data

### Amazon data

Download test data

```
curl -O http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz
```

Convert the data

```
python $GOPATH/src/github.com/bmeg/arachne/example/amazon_convert.py amazon-meta.txt.gz test.data
```

Create a graph called 'amazon'

```
arachne create amazon
```

List the graphs

```
arachne list
```

Load data

```
arachne load --edge test.data.edge --vertex test.data.vertex --graph amazon
```

__Example queries:__

_Command line_

```
arachne query amazon 'O.query().V().out()'
```

_Python_

```
import aql
import json

conn = aql.Connection("http://localhost:8201")

O = conn.graph("amazon")

# Count the Vertices
print list(O.query().V().count())
# Count the Edges
print list(O.query().E().count())

# Try simple travesral
print list(O.query().V("B00000I06U").outEdge())

# Find every Book that is similar to a DVD
for a in O.query().V().where(aql.eq("$.group", "Book")).as_("a").out("similar").where(aql.eq("$.group", "DVD")).as_("b").select(["a", "b"]):
    print a
```

### TCGA RNA Expression

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

Load pathway information

```
curl -O http://www.pathwaycommons.org/archives/PC2/v9/PathwayCommons9.All.hgnc.sif.gz
gunzip PathwayCommons9.All.hgnc.sif.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_sif.py PathwayCommons9.All.hgnc.sif
```

Load expression data

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

Query: 

```
import aql
conn = aql.Connection("http://localhost:8201")
O = conn.graph("test-data")

# Print out expression data of all Stage IIA samples
for row in O.query().\
    V().\
    where(aql.and_(aql.eq("$.label", "Sample"), aql.eq("pathologic_stage", "Stage IIA"))).\
    out("has").\
    where(aql.eq("$.label", "Data:expression"):
  print row
```

## Traversal Operations

Traversal operations help you navigate the graph:

* in
* out
* both
* inEdge
* outEdge
* bothEdge

As and select work together to keep track of state during traversals:

* as
* select

Filter operations help you cull or craft the results:

* distinct
* fields
* limit
* match
* render
* where

Aggregate operations assemble metrics from the traversal results:

* count
* aggregate

Several of the above methods (where, fields, render, etc.) reference properties of the vertices/edges during the traversal. 
We opted to use a variation on JsonPath syntax as described in http://goessner.net/articles/

__Syntax Example:__

Given the following example data:

```
{
  "current": {
    "gid": 111,
    "label": "variant",
    "data": {
      "vid": "NM_007294.3:c.4963_4981delTGGCCTGACCCCAGAAG",
      "type": "deletion"
      "publications": [
        {
          "pmid": 29480828,
          "doi": "10.1097/MD.0000000000009380"
        },
        {
          "pmid": 23666017,
          "doi": "10.1097/IGC.0b013e31829527bd"
        }
      ]
    }
  }
  "marks": {
    "gene": {
      "gid": 1,
      "label": "gene",
      "data": {
        "symbol": {
          "ensembl": "ENSG00000012048",
          "hgnc": 1100,
          "entrez": 672,
          "hugo": "BRCA1"
        }
        "transcipts": ["ENST00000471181.7", "ENST00000357654.8", "ENST00000493795.5"]
      }
    }
  }
}
```

| jsonpath                   | result              |
| :------------------------- | :------------------ |
| _gid                      | 111                 |
| _label                    | "variant"           |
| type                     | "deletion"          |
| publications[0].pmid     | 29480828            |
| publications.pmid        | [29480828, 23666017] |
| $gene._gid               | 1   |
| $gene._data.symbol.ensembl  | "ENSG00000012048"   |
| $gene.symbol.ensembl       | "ENSG00000012048"   |
| $gene.transcripts[0]       | "ENST00000471181.7" |
| $gene.transcripts[0:1]     | ["ENST00000471181.7", "ENST00000357654.8"] |


## GraphQL

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

### Loading the Schema

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
        label: friend
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

### Using built-in example

Loading the example data and the example schema:

```
arachne example
```

See the example graph

```
arachne dump --vertex --edge --graph example
```

See the example graph schema

```
arachne dump --vertex --edge --graph example:schema
```

### Example queries

Get Types:

```
curl -X POST -H "Content-Type:application/graphql" -d '{__schema{types{name}}}' http://localhost:8201/graphql/example
```

Get Info about Human object

```
curl -X POST -H "Content-Type:application/graphql" -d '{__type(name:"Human"){fields{name}}}' http://localhost:8201/graphql/example
```

Get List of all Human ids

```
curl -X POST -H "Content-Type:application/graphql" -d 'query { HumanIds }' http://localhost:8201/graphql/example
```

Get Human 1000 and list their friends

```
curl -X POST -H "Content-Type:application/graphql" -d 'query {Human(id:"1000"){name,friends{name}}}' http://localhost:8201/graphql/example
```
