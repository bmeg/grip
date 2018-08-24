---
title: Overview
menu:
  main:
    identifier: docs
    weight: -1000
---

# Overview

GRIP stands for GRaph Integration Platform. It provides a graph interface on top of a variety of existing database technologies including:
MongoDB, Elasticsearch, PostgreSQL, MySQL, MariaDB, Badger, and LevelDB.

Properties of an GRIP graph:

* Both vertices and edges in a graph can have any number of properties associated with them.
* There are many types of vertices and edges in a graph. Thus two vertices may have myriad types of edges
  connecting them reflecting myriad types of relationships.
* Edges in the graph are directed, meaning they have a source and destination.

GRIP also provides a query API for the traversing, analyzing and manipulating your graphs. Its syntax is inspired by
[Apache TinkerPop](http://tinkerpop.apache.org/). Learn more [here](/docs/queries/getting_started).
