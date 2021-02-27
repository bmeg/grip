---
title: MongoDB

menu:
  main:
    parent: Databases
    weight: 3
---

# MongoDB

GRIP supports storing vertices and edges in [MongoDB][mongo].

Config:

```yaml
Default: mongo

Drivers:
  mongo:
    MongoDB:
      URL: "mongodb://localhost:27000"
      DBName: "gripdb"
      Username: ""
      Password: ""
      UseCorePipeline: False
      BatchSize: 0
```

[mongo]: https://www.mongodb.com/

`UseCorePipeline` - Default is to use Mongo pipeline API to do graph traversals.
By enabling `UseCorePipeline`, GRIP will do the traversal logic itself, only using
Mongo for graph storage.

`BatchSize` - For core engine operations, GRIP dispatches element lookups in
batches to minimize query overhead. If missing from config file (which defaults to 0)
the engine will default to 1000.
