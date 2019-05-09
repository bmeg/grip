package example

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
)

// SWVertices are the vertices for the test graph
var SWVertices = []*gripql.Vertex{
	{Gid: "1000", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Luke Skywalker",
			"bodyMeasurements": map[string]interface{}{
				"height": 1.72,
				"mass":   77,
			},
			"homePlanet": "Tatooine",
		},
	)},
	{Gid: "1001", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Darth Vader",
			"bodyMeasurements": map[string]interface{}{
				"height": 2.02,
				"mass":   136,
			},
			"homePlanet": "Tatooine",
		},
	)},
	{Gid: "1002", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Han Solo",
			"bodyMeasurements": map[string]interface{}{
				"height": 1.8,
				"mass":   80,
			},
			"homePlanet": "Corellia",
		},
	)},
	{Gid: "1003", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Leia Organa",
			"bodyMeasurements": map[string]interface{}{
				"height": 1.5,
				"mass":   49,
			},
			"homePlanet": "Alderaan",
		},
	)},
	{Gid: "1004", Label: "Human", Data: protoutil.AsStruct(
		map[string]interface{}{
			"name": "Wilhuff Tarkin",
			"bodyMeasurements": map[string]interface{}{
				"height": 1.8,
			},
			"homePlanet": "Eriadu",
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
			"title": "A New Hope",
		},
	)},
	{Gid: "4001", Label: "Movie", Data: protoutil.AsStruct(
		map[string]interface{}{
			"title": "Empire Strikes Back",
		},
	)},
	{Gid: "4002", Label: "Movie", Data: protoutil.AsStruct(
		map[string]interface{}{
			"title": "The Return of the Jedi",
		},
	)},
}

// SWEdges are the edges for the test graph
var SWEdges = []*gripql.Edge{
	//Luke Edges
	{Label: "friendsWith", From: "1000", To: "1002"},
	{Label: "friendsWith", From: "1000", To: "1003"},
	{Label: "friendsWith", From: "1000", To: "2000"},
	{Label: "friendsWith", From: "1000", To: "2001"},
	{Label: "appearsIn", From: "1000", To: "4000"},
	{Label: "appearsIn", From: "1000", To: "4001"},
	{Label: "appearsIn", From: "1000", To: "4002"},
	{Label: "pilots", From: "1000", To: "3001"},
	{Label: "pilots", From: "1000", To: "3003"},

	//Darth Vader Edges
	{Label: "friendsWith", From: "1001", To: "1004"},
	{Label: "appearsIn", From: "1001", To: "4000"},
	{Label: "appearsIn", From: "1001", To: "4001"},
	{Label: "appearsIn", From: "1001", To: "4002"},
	{Label: "pilots", From: "1001", To: "3002"},

	//Han Solo Edges
	{Label: "friendsWith", From: "1002", To: "1000"},
	{Label: "friendsWith", From: "1002", To: "1003"},
	{Label: "friendsWith", From: "1002", To: "2001"},
	{Label: "appearsIn", From: "1002", To: "4000"},
	{Label: "appearsIn", From: "1002", To: "4001"},
	{Label: "appearsIn", From: "1002", To: "4002"},
	{Label: "pilots", From: "1002", To: "3000"},
	{Label: "pilots", From: "1002", To: "3003"},

	//Leia Organa Edges
	{Label: "friendsWith", From: "1003", To: "1000"},
	{Label: "friendsWith", From: "1003", To: "1002"},
	{Label: "friendsWith", From: "1003", To: "2000"},
	{Label: "friendsWith", From: "1003", To: "2001"},
	{Label: "appearsIn", From: "1003", To: "4000"},
	{Label: "appearsIn", From: "1003", To: "4001"},
	{Label: "appearsIn", From: "1003", To: "4002"},

	//Wilhuff Tarkin Edges
	{Label: "friendsWith", From: "1004", To: "1001"},
	{Label: "appearsIn", From: "1004", To: "4000"},
}

var schema = `
edges:
- data: {}
  from: Human
  gid: (Human)--appearsIn->(Movie)
  label: appearsIn
  to: Movie
- data: {}
  from: Human
  gid: (Human)--friendsWith->(Droid)
  label: friendsWith
  to: Droid
- data: {}
  from: Human
  gid: (Human)--friendsWith->(Human)
  label: friendsWith
  to: Human
- data: {}
  from: Human
  gid: (Human)--pilots->(Starship)
  label: pilots
  to: Starship
graph: example-graph
vertices:
- data:
    name: STRING
    primaryFunction: STRING
  gid: Droid
  label: Droid
- data:
    bodyMeasurements:
      height: NUMERIC
      mass: NUMERIC
    homePlanet: STRING
    name: STRING
  gid: Human
  label: Human
- data:
    name: STRING
  gid: Movie
  label: Movie
- data:
    length: NUMERIC
    name: STRING
  gid: Starship
  label: Starship
`

var SWSchema *gripql.Graph

func init() {
	schemas, err := gripql.ParseYAMLGraph([]byte(schema))
	if err != nil {
		panic(fmt.Errorf("Error loading example graph schema: %v", err))
	}
	SWSchema = schemas[0]
}
