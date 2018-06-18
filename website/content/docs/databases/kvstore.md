---
title: Embedded KV Store

menu:
  main:
    parent: Databases
    weight: 2
---

# Embedded Key Value Stores

Arachne supports storing vertices and edges in a variety of key-value stores including:
 
 * [Badger](https://github.com/dgraph-io/badger)
 * [BoltDB](https://github.com/boltdb/bolt)
 * [LevelDB](https://github.com/syndtr/goleveldb)
 * [RocksDB](https://rocksdb.org/)
 
Config:

```yaml
# pick one of: badger, bolt, level, rocks
Database: badger

KVStorePath: arachne.db
```
