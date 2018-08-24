---
title: GraphQL
menu:
  main:
    parent: Queries
    weight: 25
---

# GraphQL

GRIP supports GraphQL access of the property graphs. Currently this is read-only access to the graph.

**_GraphQL access is only supported when using the MongoDB driver_**

### Load built-in example graph

Loading the example data and the example schema:

```
grip load example-graph
```

See the example graph

```
grip dump example-graph --vertex --edge
```

See the example graph schema

```
grip schema example-graph
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
