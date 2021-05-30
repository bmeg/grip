---
title: Graph Model

menu:
  main:
    parent: gripper
    weight: 3
---

# GRIPPER



## Graph Model

The graph model describes how GRIP will access multiple gripper servers. The graph
is described using three sections, `sources`, `vertices` and `edges`.

The `sources` section describes all of the GRIPPER resources that GRIP will use
to build the graph. The `vertices` section describes how different collections
found in these sources will be turned into Vertex found in the graph. Finally, the
`edges` section describes the different kinds of rules that can be used build the
edges in the graph.

Edges can be built from two rules `fieldToField` and `edgeTable`. In `fieldToField`,
a field value found in one vertex can be used to look up matching destination vertices
by using an indexed field found in another collection that has been mapped to a vertex.
For `edgeTable` connections, there is a single collection that represents a connection between
two other collections that have been mapped to vertices.

Example:
```
sources:
  tableServer:
    host: localhost:50051

vertices:
  "Character:" :
    source: tableServer
    label: Character
    collection: Character

  "Planet:" :
    source: tableServer
    label: Planet
    collection: Planet

  "Film:" :
    source: tableServer
    label: Film
    collection: Film

  "Species:" :
    source: tableServer
    label: Species
    collection: Species

  "Starship:" :
    source: tableServer
    label: Starship
    collection: Starship

  "Vehicle:" :
    source: tableServer
    label: Vehicle
    collection: Vehicle

edges:
  homeworld:
    fromVertex: "Character:"
    toVertex: "Planet:"
    label: homeworld
    fieldToField:
      fromField: $.homeworld
      toField: $.id

  species:
    fromVertex: "Character:"
    toVertex: "Species:"
    label: species
    fieldToField:
      fromField: $.species
      toField: $.id

  people:
    fromVertex: "Species:"
    toVertex: "Character:"
    label: people
    edgeTable:
      source: tableServer
      collection: speciesCharacter
      fromField: $.from
      toField: $.to


  residents:
    fromVertex: "Planet:"
    toVertex: "Character:"
    label: residents
    edgeTable:
      source: tableServer
      collection: planetCharacter
      fromField: $.from
      toField: $.to

  filmVehicles:
    fromVertex: "Film:"
    toVertex: "Vehicle:"
    label: "vehicles"
    edgeTable:
      source: tableServer
      collection: filmVehicles
      fromField: "$.from"
      toField: "$.to"

  vehicleFilms:
    toVertex: "Film:"
    fromVertex: "Vehicle:"
    label: "films"
    edgeTable:
      source: tableServer
      collection: filmVehicles
      toField: "$.from"
      fromField: "$.to"

  filmStarships:
    fromVertex: "Film:"
    toVertex: "Starship:"
    label: "starships"
    edgeTable:
      source: tableServer
      collection: filmStarships
      fromField: "$.from"
      toField: "$.to"

  starshipFilms:
    toVertex: "Film:"
    fromVertex: "Starship:"
    label: "films"
    edgeTable:
      source: tableServer
      collection: filmStarships
      toField: "$.from"
      fromField: "$.to"

  filmPlanets:
    fromVertex: "Film:"
    toVertex: "Planet:"
    label: "planets"
    edgeTable:
      source: tableServer
      collection: filmPlanets
      fromField: "$.from"
      toField: "$.to"

  planetFilms:
    toVertex: "Film:"
    fromVertex: "Planet:"
    label: "films"
    edgeTable:
      source: tableServer
      collection: filmPlanets
      toField: "$.from"
      fromField: "$.to"

  filmSpecies:
    fromVertex: "Film:"
    toVertex: "Species:"
    label: "species"
    edgeTable:
      source: tableServer
      collection: filmSpecies
      fromField: "$.from"
      toField: "$.to"

  speciesFilms:
    toVertex: "Film:"
    fromVertex: "Species:"
    label: "films"
    edgeTable:
      source: tableServer
      collection: filmSpecies
      toField: "$.from"
      fromField: "$.to"

  filmCharacters:
    fromVertex: "Film:"
    toVertex: "Character:"
    label: characters
    edgeTable:
      source: tableServer
      collection: filmCharacters
      fromField: "$.from"
      toField: "$.to"

  characterFilms:
    fromVertex: "Character:"
    toVertex: "Film:"
    label: films
    edgeTable:
      source: tableServer
      collection: filmCharacters
      toField: "$.from"
      fromField: "$.to"

  characterStarships:
    fromVertex: "Character:"
    toVertex: "Starship:"
    label: "starships"
    edgeTable:
      source: tableServer
      collection: characterStarships
      fromField: "$.from"
      toField: "$.to"

  starshipCharacters:
    toVertex: "Character:"
    fromVertex: "Starship:"
    label: "pilots"
    edgeTable:
      source: tableServer
      collection: characterStarships
      toField: "$.from"
      fromField: "$.to"
```
