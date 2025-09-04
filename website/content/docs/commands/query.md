---
title: query

menu:
  main:
    parent: commands
    weight: 2
---

```
grip query <graph> <query>
```

Run a query on a graph.

Examples
```bash
grip query pc12 'V().hasLabel("Pathway").count()'
```

