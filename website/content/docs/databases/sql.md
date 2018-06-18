---
title: SQL

menu:
  main:
    parent: Databases
    weight: 4
---

# SQL

Arachne supports modeling an existing SQL database as a graph. 

Currently Arachne only supports [postgres][psql]. 

```
Database: sql

SQL:
  DataSourceName: "host=localhost port=15432 user=postgres dbname=smtest sslmode=disable"
  Driver: postgres
  
  Graphs:
    - Graph:
      Vertices:
        - Table:
          Label:
          GidField:
  
      Edges:
        - Table: 
          Label: 
          GidField: 
          From:
            SourceField:
            DestTable:
            DestField:
          To:
            SourceField:
            DestTable:
            DestField:
```

[psql]: https://www.postgresql.org/
