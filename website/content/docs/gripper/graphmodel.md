---
title: Graph Model

menu:
  main:
    parent: gripper
    weight: 3
---

# GRIPPER

GRIP Plugable External Resources

## Graph Model

The graph model describes how GRIP will access multiple gripper servers. The mapping
of these data resources is done using a graph. The `vertices` represent how each vertex
type will be mapped, and the `edges` describe how edges will be created. The `_id`
of each vertex represents the prefix domain of all vertices that can be found in that
source.

The `sources` referenced by the graph are provided to GRIP at run time, each named resource is a
different GRIPPER plugin that abstracts an external resource.
The `vertices` section describes how different collections
found in these sources will be turned into Vertex found in the graph. Finally, the
`edges` section describes the different kinds of rules that can be used build the
edges in the graph.

Edges can be built from two rules `fieldToField` and `edgeTable`. In `fieldToField`,
a field value found in one vertex can be used to look up matching destination vertices
by using an indexed field found in another collection that has been mapped to a vertex.
For `edgeTable` connections, there is a single collection that represents a connection between
two other collections that have been mapped to vertices.

## Runtime External Resource Config

External resources are passed to GRIP as command line options. For the command line:

```
grip server config.yaml --er tableServer=localhost:50051 --er pfb=localhost:50052
```

`tableServer` is a ER plugin that serves table data (see `gripper/test-graph`)
while `pfb` parses PFB based files (see https://github.com/bmeg/grip_pfb )

The `config.yaml` is

```
Default: badger

Drivers:
  badger:
    Badger: grip-badger.db

  swapi-driver:
    Gripper:
      ConfigFile: ./swapi.yaml
      Graph: swapi

```

This runs with a default `badger` based driver, but also provides a GRIPPER based
graph from the `swapi` mapping (see example graph map below).

## Example graph map

```
vertices:
  - _id: "Character:"
    _label: Character
    source: tableServer
    collection: Character

  - _id: "Planet:"
    _label: Planet
    collection: Planet
    source: tableServer

  - _id: "Film:"
    _label: Film
    collection: Film
    source: tableServer

  - _id: "Species:"
    _label: Species
    source: tableServer
    collection: Species

  - _id: "Starship:"
    _label: Starship
    source: tableServer
    collection: Starship

  - _id: "Vehicle:"
    _label: Vehicle
    source: tableServer
    collection: Vehicle

edges:
  - _id: "homeworld"
    _from: "Character:"
    _to: "Planet:"
    _label: homeworld
    fieldToField:
      fromField: $.homeworld
      toField: $.id

  - _id: species
    _from: "Character:"
    _to: "Species:"
    _label: species
    fieldToField:
      fromField: $.species
      toField: $.id

  - _id: people
    _from: "Species:"
    _to: "Character:"
    _label: people
    edgeTable:
      source: tableServer
      collection: speciesCharacter
      fromField: $.from
      toField: $.to

  - _id: residents
    _from: "Planet:"
    _to: "Character:"
    _label: residents
    edgeTable:
      source: tableServer
      collection: planetCharacter
      fromField: $.from
      toField: $.to

  - _id: filmVehicles
    _from: "Film:"
    _to: "Vehicle:"
    _label: "vehicles"
    edgeTable:
      source: tableServer
      collection: filmVehicles
      fromField: "$.from"
      toField: "$.to"

  - _id: vehicleFilms
    _to: "Film:"
    _from: "Vehicle:"
    _label: "films"
    edgeTable:
      source: tableServer
      collection: filmVehicles
      toField: "$.from"
      fromField: "$.to"

  - _id: filmStarships
    _from: "Film:"
    _to: "Starship:"
    _label: "starships"
    edgeTable:
      source: tableServer
      collection: filmStarships
      fromField: "$.from"
      toField: "$.to"

  - _id: starshipFilms
    _to: "Film:"
    _from: "Starship:"
    _label: "films"
    edgeTable:
      source: tableServer
      collection: filmStarships
      toField: "$.from"
      fromField: "$.to"

  - _id: filmPlanets
    _from: "Film:"
    _to: "Planet:"
    _label: "planets"
    edgeTable:
      source: tableServer
      collection: filmPlanets
      fromField: "$.from"
      toField: "$.to"

  - _id: planetFilms
    _to: "Film:"
    _from: "Planet:"
    _label: "films"
    edgeTable:
      source: tableServer
      collection: filmPlanets
      toField: "$.from"
      fromField: "$.to"

  - _id: filmSpecies
    _from: "Film:"
    _to: "Species:"
    _label: "species"
    edgeTable:
      source: tableServer
      collection: filmSpecies
      fromField: "$.from"
      toField: "$.to"

  - _id: speciesFilms
    _to: "Film:"
    _from: "Species:"
    _label: "films"
    edgeTable:
      source: tableServer
      collection: filmSpecies
      toField: "$.from"
      fromField: "$.to"

  - _id: filmCharacters
    _from: "Film:"
    _to: "Character:"
    _label: characters
    edgeTable:
      source: tableServer
      collection: filmCharacters
      fromField: "$.from"
      toField: "$.to"

  - _id: characterFilms
    _from: "Character:"
    _to: "Film:"
    _label: films
    edgeTable:
      source: tableServer
      collection: filmCharacters
      toField: "$.from"
      fromField: "$.to"

  - _id: characterStarships
    _from: "Character:"
    _to: "Starship:"
    _label: "starships"
    edgeTable:
      source: tableServer
      collection: characterStarships
      fromField: "$.from"
      toField: "$.to"

  - _id: starshipCharacters
    _to: "Character:"
    _from: "Starship:"
    _label: "pilots"
    edgeTable:
      source: tableServer
      collection: characterStarships
      toField: "$.from"
      fromField: "$.to"
```
