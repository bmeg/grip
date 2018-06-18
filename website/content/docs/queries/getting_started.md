---
title: Getting Started
menu:
  main:
    parent: Queries
    weight: -10
---

# Getting Started

Arachne has an API for making graph queries using structured data. Queries are defined using a series of step [operations](/docs/queries/operations)).

## Install the Python Client

```python
pip install "git+https://github.com/bmeg/arachne.git#subdirectory=aql/python"
```

Let's go through the features currently supported in the python client.

First, import the client and create a connection to an Arachne server:

```python
import aql
conn = aql.Connection('http://bmeg.io')
O = conn.graph("bmeg")
```

Now that we have a connection to a graph instance, we can use this to make all of our queries.

One of the first things you probably want to do is find some vertex out of all of the vertexes available in the system. In order to do this, we need to know something about the vertex we are looking for. To start, let's see if we can find a specific gene:

```python
print list(O.query().V().where(aql.eq("_label", "Gene")).where(aql.eq("symbol", "TP53")))
```

A couple things about this first and simplest query. We start with `O`, our arachne client instance connected to the "bmeg" graph, and create a new query with `.query()`. This query is now being constructed. You can chain along as many operations as you want, and nothing will actually get sent to the server until you print the results.

Once we make this query, we get a result:

```
[<AttrDict(
  {u'gid': u'gene:ENSG00000141510',
  u'data': {
    u'end': 7687550,
    u'description': u'tumor protein p53 [Source:HGNC Symbol%3BAcc:HGNC:11998]',
    u'symbol': u'TP53',
    u'start': 7661779,
    u'seqId': u'17',
    u'strand': u'-',
    u'id': u'ENSG00000141510',
    u'chromosome': u'17'
  },
  u'label': u'Gene'})>
]
```

This represents the vertex we queried for above. All vertexes in the system will have a similar structure, basically:

* _gid_: This represents the global identifier for this vertex. In order to draw edges between different vertexes from different data sets we need an identifier that can be constructed from available data. Often, the `gid` will be the field that you query on as a starting point for a traversal.
* _label_: The label represents the type of the vertex. All vertexes with a given label will share many property keys and edge labels, and form a logical group within the system.
* _data_: This is where all the data goes. `data` can be an arbitrary map, and these properties can be referenced during traversals.

You can also do a `where` query with a list of items using `aql.in_([...])` (other conditions exist, see the `Conditions` section below):

```python
print list(O.query().V().where(aql.eq("_label", "Gene")).where(aql.in_("symbol", ["TP53", "BRCA1"])).render({"gid": "_gid", "symbol":"symbol"}))
```

This returns both Gene vertexes:

```
[
  <AttrDict({u'symbol': u'TP53', u'gid': u'gene:ENSG00000141510'})>,
  <AttrDict({u'symbol': u'BRCA1', u'gid': u'gene:ENSG00000012048'})>
]
```

Once you are on a vertex, you can travel through that vertex's edges to find the vertexes it is connected to. Sometimes you don't even need to go all the way to the next vertex, the information on the edge between them may be sufficient.

Edges in the graph are directional, so there are both incoming and outgoing edges from each vertex, leading to other vertexes in the graph. Edges also have a _label_, which distinguishes the kind of connections different vertexes can have with one another.

Starting with gene TP53, and see what kind of other vertexes it is connected to.

```python
O.query().V().where(aql.eq("_label", "Gene")).where(aql.eq("symbol", "TP53"))
```

Here we have introduced a couple of new steps. The first is `.out()`. This starts from wherever you are in the graph at the moment and travels out along all the outgoing edges.
