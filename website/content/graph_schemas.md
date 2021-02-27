---
title: Graph Schemas
menu:
  main:
    weight: -1000
---

# Graph Schemas

Graph schemas are themselves an instance of a graph. As such, they can be traversed like any other graph.
The schemas are automatically added to the database following the naming pattern. `{graph-name}__schema__`

## Get the Schema of a Graph

The schema of a graph can be accessed via a GET request to `/v1/graph/{graph-name}/schema`

Alternatively, you can use the grip CLI. `grip schema get {graph-name}`

## Describing Graph Schemas
There are several methods for describing the schema of a graph.

- Provide the schema (as a YAML file) to the server at runtime. `grip server --schema {schema-file}`
- POST the schema `/v1/graph/{graph-name}/schema` via curl or use the CLI. `grip schema post --yaml {file} --json {file}`
- Configure GRIP to build the schema by sampling the data in each graph.

```yaml
Server:
  # Should the server periodically build the graph schemas?
  AutoBuildSchemas: true
  # How often the server should rebuild the graph schemas. Set to 0 to turn off
  SchemaRefreshInterval: "24h"
  # How many vertices/edges to inspect to infer the schema
  SchemaInspectN: 500
  # Strategy to use for selecting the vertices/edges to inspect.
  # Random if True; first N otherwise
  SchemaRandomSample: true
```

## Example schema

 ```yaml
 graph: example-graph

 edges:
- data: {}
  from: Human
  gid: (Human)--starship->(Starship)
  label: starship
  to: Starship
- data: {}
  from: Human
  gid: (Human)--friend->(Human)
  label: friend
  to: Human
- data: {}
  from: Human
  gid: (Human)--friend->(Droid)
  label: friend
  to: Droid
- data: {}
  from: Human
  gid: (Human)--appearsIn->(Movie)
  label: appearsIn
  to: Movie

vertices:
- data:
    name: STRING
  gid: Movie
  label: Movie
- data:
    length: NUMERIC
    name: STRING
  gid: Starship
  label: Starship
- data:
    name: STRING
    primaryFunction: STRING
  gid: Droid
  label: Droid
- data:
    height: NUMERIC
    homePlanet: STRING
    mass: NUMERIC
    name: STRING
  gid: Human
  label: Human
 ```
