---
title: GraphQL
menu:
  main:
    parent: Queries
    weight: 25
---

# GraphQL

Arachne supports GraphQL access of the property graphs. Currently this is read-only
access to the graph.
GraphQL graphs have a defined schema with typed fields and connections. This
schema must be defined before the graphql endpoint can access the graph.

All of the different label types in the graph are represented with a vertex of
label 'Object'. The vertex `gid` in the schema graph represents the label type
in the actual graph. Attached to each `Object` vertex is a `fields` parameter
that describes the fields and their data types.

Example Object Vertex:

```
gid: Human
label: Object
data:
  fields:
    name: String
    height: Float
    mass: Float
    homePlanet: String
```

A valid vertex this schema would map to would be:

```
gid: Luke Skywalker
label: Human
data:
  name: Luke Skywalker
  height: 1.72
  mass: 77
  homePlanet: Tatooine
```

Complex Types are described using data the schema data structures with the final
value element being a string on the data type. So an array of strings would be
coded in JSON as `["String"]`. A map of values would be
`["name" : "String", "values" : [float]]`

There is one vertex, of label `Query` that defines the root query element.
There should be one and only one declared in the schema graph. It's `gid` doesn't
matter.

Fields in objects that connect to other nodes can be defined on Object to another
with edge label `field`. The `data` for the edge needs a `name` field to declare
the field name. An optional `label` field can also be added to specify which
edge labels are followed for the field. The field will be projected as an array
of the destination object type.

To connect the `Human` object to its friends:

```
label: field
from: Human
to: Human
data:
  name: friends
  label: friend
```

### Loading the Schema

The example data would be in a file called `data.yaml`:

```
vertices:
  - gid: 1000
    label: Human
    data:
      name: Luke Skywalker
      height: 1.72
      mass: 77
      homePlanet: Tatooine
  - gid: 1001
    label: Human
    data:
      name: Darth Vader
      height: 2.02
      mass: 136
      homePlanet: Tatooine
  - gid: 1002
    label: Human
    data
      name: Han Solo
      height: 1.8
      mass: 80
  - gid: 1003
    label: Human
    data:
      name: Leia Organa
      height: 1.5
      mass: 49
      homePlanet: Alderaan
  - gid: 1004
    label: Human
    data
      name: Wilhuff Tarkin
      height: 1.8
      mass:   nil
edges:
  - {label: "friend", from: "1000", to: "1002"}
  - {label: "friend", from: "1000", to: "1003"}
  - {label: "friend", from: "1001", to: "1004"}
  - {label: "friend", from: "1002", to: "1000"}
  - {label: "friend", from: "1002", to: "1003"}
  - {label: "friend", from: "1003", to: "1000"}
  - {label: "friend", from: "1003", to: "1002"}
  - {label: "friend", from: "1004", to: "1001"}
```

For the friend network, the schema would be a file named `schema.yaml` with:

```
vertices:
  - gid: root
    label: Query
  - gid: Human
    label: Object
    data:
      fields:
        name: String
        height: Float
        mass: Float
        homePlanet: String
edges:
    - label: field
      from: root
      to: Human
      data:
        type: idQuery
        name: Human
    - label: field
      from: Human
      to: Human
      data:
        name: friends
        label: friend
    - label: field
      from: root
      to: Human
      data:
        name: HumanIds
        type: idList
```

To load the test data:

```
arachne load --graph test --yaml data.yaml
arachne load --graph test:schema --yaml schema.yaml
```

### Using built-in example

Loading the example data and the example schema:

```
arachne example
```

See the example graph

```
arachne dump --vertex --edge --graph example
```

See the example graph schema

```
arachne dump --vertex --edge --graph example-schema
```

### Example queries

Get Types:

```
curl -X POST -H "Content-Type:application/graphql" -d '{__schema{types{name}}}' http://localhost:8201/graphql/example
```

Get Info about Human object

```
curl -X POST -H "Content-Type:application/graphql" -d '{__type(name:"Human"){fields{name}}}' http://localhost:8201/graphql/example
```

Get List of all Human ids

```
curl -X POST -H "Content-Type:application/graphql" -d 'query { HumanIds }' http://localhost:8201/graphql/example
```

Get Human 1000 and list their friends

```
curl -X POST -H "Content-Type:application/graphql" -d 'query {Human(id:"1000"){name,friends{name}}}' http://localhost:8201/graphql/example
```
