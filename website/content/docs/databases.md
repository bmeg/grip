---
title: Database Configuration
menu:
  main:
    identifier: Databases
    weight: 20
---


# Embedded Key Value Stores

GRIP supports storing vertices and edges in a variety of key-value stores including:

 * [Pebble](https://github.com/cockroachdb/pebble)
 * [Badger](https://github.com/dgraph-io/badger)
 * [BoltDB](https://github.com/boltdb/bolt)
 * [LevelDB](https://github.com/syndtr/goleveldb)

Config:

```yaml
Default: kv

Driver:
  kv:
    Badger: grip.db
```

----

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

----


# GRIDS

This is an indevelopment high performance graph storage system.

Config:

```yaml
Default: db

Drivers:
  db:
    Grids: grip-grids.db

```

----

# PostgreSQL

GRIP supports storing vertices and edges in [PostgreSQL][psql].

Config:

```yaml
Default: psql

Drivers:
  psql:
    PSQL:
      Host: localhost
      Port: 15432
      User: ""
      Password: ""
      DBName: "grip"
      SSLMode: disable
```

[psql]: https://www.postgresql.org/

---

# SQLite

GRIP supports storing vertices and edges in [SQLite]

Config:

```yaml
Default: sqlite

Drivers:
  sqlite:
    Sqlite:
      DBName: tester/sqliteDB
```

[psql]: https://sqlite.org/
