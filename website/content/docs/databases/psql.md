---
title: PostgreSQL

menu:
  main:
    parent: Databases
    weight: 4
---

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
