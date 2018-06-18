---
title: Elasticsearch

menu:
  main:
    parent: Databases
    weight: 1
---

# Elasticsearch

Arachne supports storing vertices and edges in [Elasticsearch][elastic].

Config:

```yaml
Database: elasticsearch

Elasticsearch:
  URL: "http://localhost:9200"
  DBName: "arachnedb"
  Username: ""
  Password: ""
```

[elastic]: https://www.elastic.co/
