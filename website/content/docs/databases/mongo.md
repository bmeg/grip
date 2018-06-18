---
title: MongoDB

menu:
  main:
    parent: Databases
    weight: 3
---

# MongoDB

Arachne supports storing vertices and edges in [MongoDB][mongo].

Config:

```yaml
Database: mongodb

MongoDb:
  URL: "localhost:9200"
  DBName: "arachnedb"
  Username: ""
  Password: ""
```

[mongo]: https://www.mongodb.com/
