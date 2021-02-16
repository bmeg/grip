---
title: GraphQL
menu:
  main:
    parent: graphql
    weight: 25
---

# GraphQL

**GraphQL support is considered Alpha. The code is not stable and the API will likely change.**
**_GraphQL access is only supported when using the MongoDB driver_**

GRIP supports GraphQL access of the property graphs. Currently this is read-only access to the graph.


### Load built-in example graph

Loading the example data and the example schema:

```
grip load example-graph
```

See the example graph

```
grip dump example-graph --vertex --edge
```

Sample components of the graph to produce a schema and store to a file
```
grip schema sample example-graph > test.schema
```

You may want to edit the schema, but if it seems correct, post it to the server:

```
./grip schema post --json test.schema
```

See the graph schema

```
grip schema get example-graph
```

### Example queries

Get Types:

```
curl -X POST -H "Content-Type:application/graphql" -d '{__schema{types{name}}}' http://localhost:8201/graphql/example-graph
```

Get Info about Human object

```
curl -X POST -H "Content-Type:application/graphql" -d '{__type(name:"Human"){fields{name}}}' http://localhost:8201/graphql/example-graph
```

Get List of all Human ids

```
curl -X POST -H "Content-Type:application/graphql" -d 'query {Human{id}}' http://localhost:8201/graphql/example-graph
```

Get Human 1000 and list their friends

```
curl -X POST -H "Content-Type:application/graphql" -d 'query {Human(id:"1000"){name,friend_to_Human{name}}}' http://localhost:8201/graphql/example-graph
```
