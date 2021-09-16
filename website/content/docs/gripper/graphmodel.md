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
type will be mapped, and the `edges` describe how edges will be created. The `gid`
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
  - gid: "Character:"
    label: Character
    data:
      source: tableServer
      collection: Character

  - gid: "Planet:"
    label: Planet
    data:
      collection: Planet
      source: tableServer

  - gid: "Film:"
    label: Film
    data:
      collection: Film
      source: tableServer

  - gid: "Species:"
    label: Species
    data:
      source: tableServer
      collection: Species

  - gid: "Starship:"
    label: Starship
    data:
      source: tableServer
      collection: Starship

  - gid: "Vehicle:"
    label: Vehicle
    data:
      source: tableServer
      collection: Vehicle

edges:
  - gid: "homeworld"
    from: "Character:"
    to: "Planet:"
    label: homeworld
    data:
      fieldToField:
        fromField: $.homeworld
        toField: $.id

  - gid: species
    from: "Character:"
    to: "Species:"
    label: species
    data:
      fieldToField:
        fromField: $.species
        toField: $.id

  - gid: people
    from: "Species:"
    to: "Character:"
    label: people
    data:
      edgeTable:
        source: tableServer
        collection: speciesCharacter
        fromField: $.from
        toField: $.to

  - gid: residents
    from: "Planet:"
    to: "Character:"
    label: residents
    data:
      edgeTable:
        source: tableServer
        collection: planetCharacter
        fromField: $.from
        toField: $.to

  - gid: filmVehicles
    from: "Film:"
    to: "Vehicle:"
    label: "vehicles"
    data:
      edgeTable:
        source: tableServer
        collection: filmVehicles
        fromField: "$.from"
        toField: "$.to"

  - gid: vehicleFilms
    to: "Film:"
    from: "Vehicle:"
    label: "films"
    data:
      edgeTable:
        source: tableServer
        collection: filmVehicles
        toField: "$.from"
        fromField: "$.to"

  - gid: filmStarships
    from: "Film:"
    to: "Starship:"
    label: "starships"
    data:
      edgeTable:
        source: tableServer
        collection: filmStarships
        fromField: "$.from"
        toField: "$.to"

  - gid: starshipFilms
    to: "Film:"
    from: "Starship:"
    label: "films"
    data:
      edgeTable:
        source: tableServer
        collection: filmStarships
        toField: "$.from"
        fromField: "$.to"

  - gid: filmPlanets
    from: "Film:"
    to: "Planet:"
    label: "planets"
    data:
      edgeTable:
        source: tableServer
        collection: filmPlanets
        fromField: "$.from"
        toField: "$.to"

  - gid: planetFilms
    to: "Film:"
    from: "Planet:"
    label: "films"
    data:
      edgeTable:
        source: tableServer
        collection: filmPlanets
        toField: "$.from"
        fromField: "$.to"

  - gid: filmSpecies
    from: "Film:"
    to: "Species:"
    label: "species"
    data:
      edgeTable:
        source: tableServer
        collection: filmSpecies
        fromField: "$.from"
        toField: "$.to"

  - gid: speciesFilms
    to: "Film:"
    from: "Species:"
    label: "films"
    data:
      edgeTable:
        source: tableServer
        collection: filmSpecies
        toField: "$.from"
        fromField: "$.to"

  - gid: filmCharacters
    from: "Film:"
    to: "Character:"
    label: characters
    data:
      edgeTable:
        source: tableServer
        collection: filmCharacters
        fromField: "$.from"
        toField: "$.to"

  - gid: characterFilms
    from: "Character:"
    to: "Film:"
    label: films
    data:
      edgeTable:
        source: tableServer
        collection: filmCharacters
        toField: "$.from"
        fromField: "$.to"

  - gid: characterStarships
    from: "Character:"
    to: "Starship:"
    label: "starships"
    data:
      edgeTable:
        source: tableServer
        collection: characterStarships
        fromField: "$.from"
        toField: "$.to"

  - gid: starshipCharacters
    to: "Character:"
    from: "Starship:"
    label: "pilots"
    data:
      edgeTable:
        source: tableServer
        collection: characterStarships
        toField: "$.from"
        fromField: "$.to"
```
