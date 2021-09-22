[![Build Status](https://travis-ci.org/bmeg/grip.svg?branch=master)](https://travis-ci.org/bmeg/grip)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Godoc](https://img.shields.io/badge/godoc-ref-blue.svg)](http://godoc.org/github.com/bmeg/grip)
[![Gitter](https://badges.gitter.im/bmeg/grip.svg)](https://gitter.im/bmeg/grip)

# GRIP

https://bmeg.github.io/grip/

GRIP stands for GRaph Integration Platform. It provides a graph interface on top of a variety of existing database technologies including: MongoDB, Elasticsearch, PostgreSQL, MySQL, MariaDB, Badger, and LevelDB.

Properties of an GRIP graph:

* Both vertices and edges in a graph can have any number of properties associated with them.
* There are many types of vertices and edges in a graph. Thus two vertices may have myriad types of edges
  connecting them reflecting myriad types of relationships.
* Edges in the graph are directed, meaning they have a source and destination.

GRIP also provides a query API for the traversing, analyzing and manipulating your graphs. Its syntax is inspired by
[Apache TinkerPop](http://tinkerpop.apache.org/). Learn more [here](https://bmeg.github.io/grip/).



## Pathway Commons
To load Pathway commons into a local instance of GRIP, first download the Pathway commons source file.
```
curl -O https://www.pathwaycommons.org/archives/PC2/v12/PathwayCommons12.All.BIOPAX.owl.gz
```

Start grip server (using Pebble driver)
```
grip server --driver=pebble
```

In another terminal, create the graph
```
grip create pc12
```

And load the file, using the RDF loader
```
grip rdf --gzip pc12 PathwayCommons12.All.BIOPAX.owl.gz -m "http://www.biopax.org/release/biopax-level3.owl#=" -m "http://pathwaycommons.org/pc12/#=pc12:"
```

Once the graph has been loaded into the database, you can view all of the
different vertex and edge types in the graph:
```
grip list labels pc12
```

Or run an example query, such as count all of the pathways:
```
grip query pc12 'V().hasLabel("Pathway").count()'
```
