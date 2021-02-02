---
title: Elasticsearch

menu:
  main:
    parent: Databases
    weight: 1
---

# Elasticsearch

GRIP supports storing vertices and edges in [Elasticsearch][elastic].

Config:

```yaml
Default: elastic

Drivers:
  elastic:
    Elasticsearch:
      URL: "http://localhost:9200"
      DBName: "gripdb"
      Username: ""
      Password: ""
```

[elastic]: https://www.elastic.co/
