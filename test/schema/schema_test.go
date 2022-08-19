package schema

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/gripql/example"

	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/server"
)

func TestSchemaScanner(t *testing.T) {

	graph := "test-graph"

	conf := config.DefaultConfig()
	config.TestifyConfig(conf)

	kv, err := kvi.NewKVInterface("badger", conf.KVStorePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		kv.Close()
		os.RemoveAll(conf.KVStorePath)
		os.RemoveAll(conf.Server.WorkDir)
	}()

	db := kvgraph.NewKVGraph(kv)
	srv, err := server.NewGripServer(db, conf.Server, nil)
	if err != nil {
		t.Fatal(err)
	}

	queryClient := gripql.NewQueryDirectClient(srv)
	editClient := gripql.NewEditDirectClient(srv)
	client := gripql.WrapClient(queryClient, editClient)

	err = client.AddGraph(graph)
	if err != nil {
		t.Fatal(err)
	}

	elemChan := make(chan *gripql.GraphElement)
	wait := make(chan bool)
	go func() {
		if err := client.BulkAdd(elemChan); err != nil {
			fmt.Printf("%s", err)
		}
		wait <- false
	}()

	for _, v := range example.SWVertices {
		elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
	}
	for _, e := range example.SWEdges {
		elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
	}

	close(elemChan)
	<-wait

	var exclude []string
	graphSchema, err := ScanSchema(client, graph, 50, exclude)
	if err != nil {
		t.Fatal(err)
	}

	if len(graphSchema.Vertices) != 4 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Vertices), 4)
	}
	for _, v := range graphSchema.Vertices {
		switch v.Gid {
		case "Human", "Droid", "Starship", "Movie":
		default:
			t.Errorf("Unexpected type %s ", v.Gid)
		}
	}
	if len(graphSchema.Edges) != 4 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Edges), 4)
	}
	for _, v := range graphSchema.Edges {
		switch v.Label {
		case "friend", "starship", "appearsIn":
		default:
			t.Log(v)
			t.Errorf("Unexpected type %s ", v.Label)
		}
	}

	exclude = []string{"Movie"}
	graphSchema, err = ScanSchema(client, graph, 50, exclude)
	if err != nil {
		t.Fatal(err)
	}

	if len(graphSchema.Vertices) != 3 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Vertices), 3)
	}
	for _, v := range graphSchema.Vertices {
		switch v.Gid {
		case "Human", "Droid", "Starship":
		default:
			t.Errorf("Unexpected type %s ", v.Gid)
		}
	}
	if len(graphSchema.Edges) != 4 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Edges), 4)
	}
	for _, v := range graphSchema.Edges {
		switch v.Label {
		case "appearsIn", "friend", "starship":
		default:
			t.Errorf("Unexpected type %s", v.Label)
		}
	}

	exclude = []string{"Movie", "appearsIn"}
	graphSchema, err = ScanSchema(client, graph, 50, exclude)
	if err != nil {
		t.Fatal(err)
	}

	if len(graphSchema.Vertices) != 3 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Vertices), 3)
	}
	for _, v := range graphSchema.Vertices {
		switch v.Gid {
		case "Human", "Droid", "Starship":
		default:
			t.Errorf("Unexpected type %s ", v.Gid)
		}
	}
	if len(graphSchema.Edges) != 3 {
		t.Errorf("unexpected edge labels: %d != %d", len(graphSchema.Edges), 3)
	}
	for _, v := range graphSchema.Edges {
		switch v.Label {
		case "friend", "starship":
		default:
			t.Errorf("Unexpected type %s", v.Label)
		}
	}
}
