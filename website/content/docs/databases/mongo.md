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

MongoDB:
  URL: "localhost:27017"
  DBName: "arachnedb"
  Username: ""
  Password: ""
```

[mongo]: https://www.mongodb.com/
