---
title: GRIPPER

menu:
  main:
    parent: Databases
    weight: 6
---

# GRIPPER

GRIP Plugable External Resources are data systems that GRIP can combine together
to create graphs.

Example:

```yaml
Drivers:
  swapi-driver:
    Gripper:
      ConfigFile: ./swapi.yaml
      Graph: swapi
```


`ConfigFile` - Path to GRIPPER graph map

`Graph` - Name of the graph for the mapped external resources.
