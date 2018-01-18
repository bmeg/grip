
package example

import (
  "github.com/bmeg/arachne/aql"
  "github.com/bmeg/arachne/protoutil"
)


var sw_vertices = []aql.Vertex{
  aql.Vertex{Gid:"1000", Label:"Human", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Luke Skywalker",
        "height": 1.72,
        "mass": 77,
        "homePlanet": "Tatooine",
      },
  )},
  aql.Vertex{Gid:"1001", Label:"Human", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Darth Vader",
        "height": 2.02,
        "mass": 136,
        "homePlanet": "Tatooine",
      },
  )},
  aql.Vertex{Gid:"1002", Label:"Human", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Han Solo",
        "height": 1.8,
        "mass": 80,
      },
  )},
  aql.Vertex{Gid:"1003", Label:"Human", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Leia Organa",
        "height": 1.5,
        "mass": 49,
        "homePlanet": "Alderaan",
      },
  )},
  aql.Vertex{Gid:"1004", Label:"Human", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Wilhuff Tarkin",
        "height": 1.8,
        "mass": nil,
      },
  )},
  aql.Vertex{Gid:"2000", Label:"Droid", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "C-3PO",
        "primaryFunction": "Protocol",
      },
  )},
  aql.Vertex{Gid:"2001", Label:"Droid", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "R2-D2",
        "primaryFunction": "Astromech",
      },
  )},
  aql.Vertex{Gid:"3000", Label:"Starship", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Millenium Falcon",
        "length": 34.37,
      },
  )},
  aql.Vertex{Gid:"3001", Label:"Starship", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "X-Wing",
        "length": 34.37,
      },
  )},
  aql.Vertex{Gid:"3002", Label:"Starship", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "TIE Advanced x1",
        "length": 9.2,
      },
  )},
  aql.Vertex{Gid:"3003", Label:"Starship", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Imperial shuttle",
        "length": 20,
      },
  )},
  aql.Vertex{Gid:"4000", Label:"Movie", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "A New Hope",
      },
  )},
  aql.Vertex{Gid:"4001", Label:"Movie", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "Empire Strikes Back",
      },
  )},
  aql.Vertex{Gid:"4002", Label:"Movie", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name": "The Return of the Jedi",
      },
  )},
}


var sw_edges = []aql.Edge{
  //Luke Edges
  aql.Edge{Label:"friend", From:"1000", To:"1002"},
  aql.Edge{Label:"friend", From:"1000", To:"1003"},
  aql.Edge{Label:"friend", From:"1000", To:"2000"},
  aql.Edge{Label:"friend", From:"1000", To:"2001"},
  aql.Edge{Label:"appearsIn", From:"1000", To:"4000"},
  aql.Edge{Label:"appearsIn", From:"1000", To:"4001"},
  aql.Edge{Label:"appearsIn", From:"1000", To:"4002"},
  aql.Edge{Label:"starship", From:"1000", To:"3001"},
  aql.Edge{Label:"starship", From:"1000", To:"3003"},

  //Darth Vader Edges
  aql.Edge{Label:"friend", From:"1001", To:"1004"},
  aql.Edge{Label:"appearsIn", From:"1001", To:"4000"},
  aql.Edge{Label:"appearsIn", From:"1001", To:"4001"},
  aql.Edge{Label:"appearsIn", From:"1001", To:"4002"},
  aql.Edge{Label:"starship", From:"1001", To:"3002"},

  //Han Solo Edges
  aql.Edge{Label:"friend", From:"1002", To:"1000"},
  aql.Edge{Label:"friend", From:"1002", To:"1003"},
  aql.Edge{Label:"friend", From:"1002", To:"2001"},
  aql.Edge{Label:"appearsIn", From:"1002", To:"4000"},
  aql.Edge{Label:"appearsIn", From:"1002", To:"4001"},
  aql.Edge{Label:"appearsIn", From:"1002", To:"4002"},
  aql.Edge{Label:"starship", From:"1002", To:"3000"},
  aql.Edge{Label:"starship", From:"1002", To:"3003"},

  //Leia Organa Edges
  aql.Edge{Label:"friend", From:"1003", To:"1000"},
  aql.Edge{Label:"friend", From:"1003", To:"1002"},
  aql.Edge{Label:"friend", From:"1003", To:"2000"},
  aql.Edge{Label:"friend", From:"1003", To:"2001"},
  aql.Edge{Label:"appearsIn", From:"1003", To:"4000"},
  aql.Edge{Label:"appearsIn", From:"1003", To:"4001"},
  aql.Edge{Label:"appearsIn", From:"1003", To:"4002"},

  //Wilhuff Tarkin Edges
  aql.Edge{Label:"friend", From:"1004", To:"1001"},
  aql.Edge{Label:"appearsIn", From:"1003", To:"4000"},
}

var sw_gql_vertices = []aql.Vertex{
  aql.Vertex{Gid:"HumanObject", Label:"Object", Data:protoutil.AsStruct(
      map[string]interface{}{
        "label" : "Human",
        "name" : "Human",
        "fields" : map[string]interface{}{
          "name" : "String",
          "height" : "Float",
          "mass" : "Float",
          "homePlanet" : "String",
        },
      },
  )},
  aql.Vertex{Gid:"DroidObject", Label:"Object", Data:protoutil.AsStruct(
      map[string]interface{}{
        "label" : "Droid",
        "name" : "Droid",
        "fields" : map[string]interface{}{
          "name" : "String",
          "primaryFunction" : "String",
        },
      },
  )},
  aql.Vertex{Gid:"StarshipObject", Label:"Object", Data:protoutil.AsStruct(
      map[string]interface{}{
        "label" : "Starship",
        "name" : "Starship",
        "fields" : map[string]interface{}{
          "name" : "String",
          "length" : "Float",
        },
      },
  )},
  aql.Vertex{Gid:"HumanQuery", Label:"Query", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name" : "Human",
      },
  )},
  aql.Vertex{Gid:"DroidQuery", Label:"Query", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name" : "Droid",
      },
  )},
}

var sw_gql_edges = []aql.Edge{
  aql.Edge{Label:"field", From:"HumanQuery", To:"HumanObject", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name" : "Human",
      },
    ),
  },
  aql.Edge{Label:"field", From:"DroidQuery", To:"DroidObject", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name" : "Droid",
      },
    ),
  },
  aql.Edge{Label:"field", From:"HumanObject", To:"HumanObject", Data:protoutil.AsStruct(
      map[string]interface{}{
        "name" : "Human",
      },
    ),
  },
}
