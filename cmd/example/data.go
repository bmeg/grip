package example

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

var swVertices = []aql.Vertex{
	{Gid: "1000", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":       "Luke Skywalker",
			"height":     1.72,
			"mass":       77,
			"homePlanet": "Tatooine",
		},
	)},
	{Gid: "1001", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":       "Darth Vader",
			"height":     2.02,
			"mass":       136,
			"homePlanet": "Tatooine",
		},
	)},
	{Gid: "1002", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "Han Solo",
			"height": 1.8,
			"mass":   80,
		},
	)},
	{Gid: "1003", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":       "Leia Organa",
			"height":     1.5,
			"mass":       49,
			"homePlanet": "Alderaan",
		},
	)},
	{Gid: "1004", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "Wilhuff Tarkin",
			"height": 1.8,
			"mass":   nil,
		},
	)},
	{Gid: "2000", Label: "Droid", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":            "C-3PO",
			"primaryFunction": "Protocol",
		},
	)},
	{Gid: "2001", Label: "Droid", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":            "R2-D2",
			"primaryFunction": "Astromech",
		},
	)},
	{Gid: "3000", Label: "Starship", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "Millennium Falcon",
			"length": 34.37,
		},
	)},
	{Gid: "3001", Label: "Starship", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "X-Wing",
			"length": 34.37,
		},
	)},
	{Gid: "3002", Label: "Starship", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "TIE Advanced x1",
			"length": 9.2,
		},
	)},
	{Gid: "3003", Label: "Starship", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name":   "Imperial shuttle",
			"length": 20,
		},
	)},
	{Gid: "4000", Label: "Movie", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "A New Hope",
		},
	)},
	{Gid: "4001", Label: "Movie", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Empire Strikes Back",
		},
	)},
	{Gid: "4002", Label: "Movie", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "The Return of the Jedi",
		},
	)},
}

var swEdges = []aql.Edge{
	//Luke Edges
	{Label: "friend", From: "1000", To: "1002"},
	{Label: "friend", From: "1000", To: "1003"},
	{Label: "friend", From: "1000", To: "2000"},
	{Label: "friend", From: "1000", To: "2001"},
	{Label: "appearsIn", From: "1000", To: "4000"},
	{Label: "appearsIn", From: "1000", To: "4001"},
	{Label: "appearsIn", From: "1000", To: "4002"},
	{Label: "starship", From: "1000", To: "3001"},
	{Label: "starship", From: "1000", To: "3003"},

	//Darth Vader Edges
	{Label: "friend", From: "1001", To: "1004"},
	{Label: "appearsIn", From: "1001", To: "4000"},
	{Label: "appearsIn", From: "1001", To: "4001"},
	{Label: "appearsIn", From: "1001", To: "4002"},
	{Label: "starship", From: "1001", To: "3002"},

	//Han Solo Edges
	{Label: "friend", From: "1002", To: "1000"},
	{Label: "friend", From: "1002", To: "1003"},
	{Label: "friend", From: "1002", To: "2001"},
	{Label: "appearsIn", From: "1002", To: "4000"},
	{Label: "appearsIn", From: "1002", To: "4001"},
	{Label: "appearsIn", From: "1002", To: "4002"},
	{Label: "starship", From: "1002", To: "3000"},
	{Label: "starship", From: "1002", To: "3003"},

	//Leia Organa Edges
	{Label: "friend", From: "1003", To: "1000"},
	{Label: "friend", From: "1003", To: "1002"},
	{Label: "friend", From: "1003", To: "2000"},
	{Label: "friend", From: "1003", To: "2001"},
	{Label: "appearsIn", From: "1003", To: "4000"},
	{Label: "appearsIn", From: "1003", To: "4001"},
	{Label: "appearsIn", From: "1003", To: "4002"},

	//Wilhuff Tarkin Edges
	{Label: "friend", From: "1004", To: "1001"},
	{Label: "appearsIn", From: "1003", To: "4000"},
}

var swGQLGraph = `{
	"vertices": [{
			"gid": "HumanObject",
			"label": "Object",
			"data": {
				"label": "Human",
				"name": "Human",
				"fields": {
					"name": "String",
					"height": "Float",
					"mass": "Float",
					"homePlanet": "String"
				}
			}
		},
		{
			"gid": "DroidObject",
			"label": "Object",
			"data": {
				"label": "Droid",
				"name": "Droid",
				"fields": {
					"name": "String",
					"primaryFunction": "String"
				}
			}
		},
		{
			"gid": "StarshipObject",
			"label": "Object",
			"data": {
				"label": "Starship",
				"name": "Starship",
				"fields": {
					"name": "String",
					"length": "Float"
				}
			}
		},
		{
			"gid": "HumanQuery",
			"label": "Query",
			"data": {
				"name": "Human"
			}
		},
		{
			"gid": "DroidQuery",
			"label": "Query",
			"data": {
				"name": "Droid"
			}
		}
	],
	"edges": [{
			"label": "field",
			"from": "HumanQuery",
			"to": "HumanObject",
			"data": {
				"name": "Human"
			}
		},
		{
			"label": "field",
			"from": "DroidQuery",
			"to": "DroidObject",
			"data": {
				"name": "Droid"
			}
		},
		{
			"label": "field",
			"from": "HumanObject",
			"to": "HumanObject",
			"data": {
				"name": "friend"
			}
		}
	]
}
`
