---
title: Record Transforms
menu:
  main:
    parent: Queries
    weight: 5
---


# Record Manipulation

## `.unwind(fields)`
Expands an array-valued field into multiple rows, one for each element in the array.
Example:

Graph
```python
{"vertex" : {"_id":"1", "_label":"Thing", "stuff" : ["1", "2", "3"]}}
```

Query
```python
G.V("1").unwind("stuff")
```

Result
```json
{"_id":"1", "_label":"Thing", "stuff" : "1"}
{"_id":"1", "_label":"Thing", "stuff" : "2"}
{"_id":"1", "_label":"Thing", "stuff" : "3"}
```

## `.group({"dest":"field"})`
Collect all travelers that are on the same element while aggregating specific fields

For the example:
```python
G.V().hasLabel("Planet").as_("planet").out("residents").as_("character").select("planet").group( {"people" : "$character.name"} )
```

All of the travelers that start on the same planet go out to residents, collect them using the `as_` and then returning to the origin

using the `select` statement. The group statement aggrigates the `name` fields from the character nodes that were visited and collects them

into a list named `people` that is added to the current planet node.

Output:
```json
{
  "vertex": {
    "_id": "Planet:2",
    "_label": "Planet",
    "climate": "temperate",
    "diameter": 12500,
    "gravity": null,
    "name": "Alderaan",
    "orbital_period": 364,
    "people": [
      "Leia Organa",
      "Raymus Antilles"
    ],
    "population": 2000000000,
    "rotation_period": 24,
    "surface_water": 40,
    "system": {
      "created": "2014-12-10T11:35:48.479000Z",
      "edited": "2014-12-20T20:58:18.420000Z"
    },
    "terrain": [
      "grasslands",
      "mountains"
    ],
    "url": "https://swapi.co/api/planets/2/"
  }
}
{
  "vertex": {
    "_id": "Planet:1",
    "_label": "Planet",
    "climate": "arid",
    "diameter": 10465,
    "gravity": null,
    "name": "Tatooine",
    "orbital_period": 304,
    "people": [
      "Luke Skywalker",
      "C-3PO",
      "Darth Vader",
      "Owen Lars",
      "Beru Whitesun lars",
      "R5-D4",
      "Biggs Darklighter"
    ],
    "population": 200000,
    "rotation_period": 23,
    "surface_water": 1,
    "system": {
      "created": "2014-12-09T13:50:49.641000Z",
      "edited": "2014-12-21T20:48:04.175778Z"
    },
    "terrain": [
      "desert"
    ],
    "url": "https://swapi.co/api/planets/1/"
  }
}
```

## `.pivot(id, key, value)`

Aggregate fields across multiple records into a single record using a pivot operations. A pivot is
an operation where a two column matrix, with one columns for keys and another column for values, is
transformed so that the keys are used to name the columns and the values are put in those columns.

So the stream of vertices:

```
{"_id":"observation_a1", "_label":"Observation", "subject":"Alice", "key":"age", "value":36}
{"_id":"observation_a2", "_label":"Observation", "subject":"Alice", "key":"sex", "value":"Female"}
{"_id":"observation_a3", "_label":"Observation", "subject":"Alice", "key":"blood_pressure", "value":"111/78"}
{"_id":"observation_b1", "_label":"Observation", "subject":"Bob", "key":"age", "value":42}
{"_id":"observation_b2", "_label":"Observation", "subject":"Bob", "key":"sex", "value":"Male"}
{"_id":"observation_b3", "_label":"Observation", "subject":"Bob", "key":"blood_pressure", "value":"120/80"}
```

with `.pivot("subject", "key", "value")` will produce:

```
{"_id":"Alice", "age":36, "sex":"Female", "blood_pressure":"111/78"}
{"_id":"Bob", "age":42, "sex":"Male", "blood_pressure":"120/80"}
```
