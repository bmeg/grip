---
title: SQLite

menu:
  main:
    parent: Databases
    weight: 4
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
