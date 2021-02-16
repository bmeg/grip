---
title: Overview
menu:
  main:
    identifier: docs
    weight: 1
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

# Supported Systems

GRIP has three types of drivers:

 - *Embedded* : GRIP has built in graph storage, built using embedded databases. This allows you to quick
 set graphs for data storage and analysis, without having to configure an external database and connect it
 to GRIP. This is the default mode if GRIP is started without any configuration. The primarily driver
 is based on [Badger](https://github.com/dgraph-io/badger).
 - *Database Backends* : GRIP can utilize a number of different database backends to store graph data.
 In this mode, GRIP formats the database and manages the data. It provides a full Read/Write API, the same
 as the Embedded deployment. Supported systems include Mongo, Portgres SQL and Elastic Search. When deployed
 using Mongo, GRIP will translate GripQL queries into [Mongo Aggregation Pipeline queries](https://docs.mongodb.com/manual/core/aggregation-pipeline/).
  - *Plugin Existing Resources* : This mode is designed to take advantage of existing data resources, mapping
 them into a graph framework so they can be accessed using GripQL. This mode is currently read-only.

# Future Plans

GRIP is still under active development and there are plans for a number of additional features. These include:

 - Batch queries. Ability to submit long running queries to be processed asynchronously and stored for later retrieval.
 - Query Caching.
 - Query Continuation. Ability to describe a graph query that starts off where another query ended. For example,
 from batch or cached query and to filtering or a few additional steps.
 - Optimized Plugable External Resource API. Adding additional operations, such as 'joins' or better filtering, to
 external resources would allow GRIP to better optimize queries and reduce query time.
 - Managed Plugin deployment.
