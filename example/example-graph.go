package example

import (
	"context"
	"fmt"
	"os"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/server"
)

// SWVertices are the vertices for the test graph
var SWVertices = []*gripql.Vertex{
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

// SWEdges are the edges for the test graph
var SWEdges = []*gripql.Edge{
	//Luke Edges
	{Label: "friend", From: "1000", To: "1002"},
	{Label: "friend", From: "1000", To: "1003"},
	{Label: "friend", From: "1000", To: "2000"},
	{Label: "friend", From: "1000", To: "2001"},
	{Label: "appearsIn", From: "1000", To: "4000"},
	{Label: "appearsIn", From: "1000", To: "4001"},
	{Label: "appearsIn", From: "1000", To: "4002"},
	{Label: "pilots", From: "1000", To: "3001"},
	{Label: "pilots", From: "1000", To: "3003"},

	//Darth Vader Edges
	{Label: "friend", From: "1001", To: "1004"},
	{Label: "appearsIn", From: "1001", To: "4000"},
	{Label: "appearsIn", From: "1001", To: "4001"},
	{Label: "appearsIn", From: "1001", To: "4002"},
	{Label: "pilots", From: "1001", To: "3002"},

	//Han Solo Edges
	{Label: "friend", From: "1002", To: "1000"},
	{Label: "friend", From: "1002", To: "1003"},
	{Label: "friend", From: "1002", To: "2001"},
	{Label: "appearsIn", From: "1002", To: "4000"},
	{Label: "appearsIn", From: "1002", To: "4001"},
	{Label: "appearsIn", From: "1002", To: "4002"},
	{Label: "pilots", From: "1002", To: "3000"},
	{Label: "pilots", From: "1002", To: "3003"},

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
  gid: (Human)--friend->(Droid)
  label: friend
  to: Droid
- data: {}
  from: Human
  gid: (Human)--friend->(Human)
  label: friend
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
    height: NUMERIC
    homePlanet: STRING
    mass: NUMERIC
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

func StartTestServer(ctx context.Context, conf *config.Config, graph string) (gripql.Client, error) {
	kv, err := kvgraph.NewKVInterface("badger", conf.KVStorePath, nil)
	if err != nil {
		return gripql.Client{}, err
	}

	db := kvgraph.NewKVGraph(kv)
	srv, err := server.NewGripServer(db, conf.Server, nil)
	if err != nil {
		return gripql.Client{}, err
	}

	queryClient := gripql.NewQueryDirectClient(srv)
	editClient := gripql.NewEditDirectClient(srv)
	client := gripql.WrapClient(queryClient, editClient)

	err = client.AddGraph(graph)
	if err != nil {
		return gripql.Client{}, err
	}

	elemChan := make(chan *gripql.GraphElement)
	wait := make(chan bool)
	go func() {
		if err := client.BulkAdd(elemChan); err != nil {
			fmt.Printf("BulkAdd error: %v", err)
		}
		wait <- false
	}()

	for _, v := range SWVertices {
		elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
	}

	for _, e := range SWEdges {
		elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
	}

	close(elemChan)
	<-wait

	go func() {
		select {
		case <-ctx.Done():
			kv.Close()
			os.RemoveAll(conf.Server.WorkDir)
			os.RemoveAll(conf.KVStorePath)
		}
	}()

	return client, nil
}
