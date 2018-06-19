[![Build Status](https://travis-ci.org/ohsu-comp-bio/funnel.svg?branch=master)](https://travis-ci.org/bmeg/arachne)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Godoc](https://img.shields.io/badge/godoc-ref-blue.svg)](http://godoc.org/github.com/bmeg/arachne)
[![Gitter](https://badges.gitter.im/bmeg/arachne.svg)](https://gitter.im/bmeg/arachne)

# Arachne

https://bmeg.github.io/arachne/

Arachne is a graph database server. It provides a graph interface on top of a variety of existing database technologies including: 
MongoDB, Elasticsearch, PostgreSQL, MySQL, MariaDB, Badger, and LevelDB.

Properties of an Arachne graph:

* Both vertices and edges in a graph can have any number of properties associated with them. 
* There are many types of vertices and edges in a graph. Thus two vertices may have myriad types of edges 
  connecting them reflecting myriad types of relationships.
* Edges in the graph are directed, meaning they have a source and destination. 

Arachne also provides a query API for the traversing, analyzing and manipulating your graphs. Its syntax is inspired by 
[Apache TinkerPop](http://tinkerpop.apache.org/). Learn more [here](https://bmeg.github.io/arachne/docs/queries/getting_started).
