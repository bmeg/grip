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

## Post a graph schema

A schema can be attached to an existing graph via a POST request to `/v1/graph/{graph-name}/schema`

Alternatively, you can use the grip CLI. `grip schema post [graph_name] --jsonSchema {file}`

Schemas must be loaded as a json file in JSON schema format. see [jsonschema](https://json-schema.org/) spec for more details

## Raw bulk loading

Once a schema is attached to a graph, raw json records can be loaded directly to grip without having to be in native grip vertex/edge format.
Schema validation is enforced when using this POST `/v1/rawJson` method.

A grip CLI alternative is also available with `grip jsonload [ndjson_file_path] [graph_name]`
See https://github.com/bmeg/grip/blob/develop/conformance/tests/ot_bulk_raw.py for a full example using gripql python package.
