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
Database: mongodb

MongoDB:
  URL: "localhost:27017"
  DBName: "gripdb"
  Username: ""
  Password: ""
```

[mongo]: https://www.mongodb.com/
