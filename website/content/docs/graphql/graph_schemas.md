---
title: Graph Schemas
menu:
  main:
    parent: graphql
    weight: 30
---

# Graph Schemas
Most GRIP based graphs are not required to have a strict schema. However, GraphQL requires
a graph schema as part of it's API. To utilize the GraphQL endpoint, there must be a
Graph Schema provided to be used by the GRIP engine to determine how to render a GraphQL endpoint.
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
- _id: (Human)--starship->(Starship)
  _label: starship
  _from: Human
  _to: Starship
- _id: (Human)--friend->(Human)
  _label: friend
  _from: Human
  _to: Human
- _id: (Human)--friend->(Droid)
  _label: friend
  _from: Human
  _to: Droid
- _id: (Human)--appearsIn->(Movie)
  _label: appearsIn
  _from: Human
  _to: Movie

vertices:
- _id: Movie
  _label: Movie
  name: STRING
- _id: Starship
  _label: Starship
  length: NUMERIC
  name: STRING
- _id: Droid
  _label: Droid
  name: STRING
  primaryFunction: STRING
- _id: Human
  _label: Human
  height: NUMERIC
  homePlanet: STRING
  mass: NUMERIC
  name: STRING
```
