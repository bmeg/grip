package schema

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/example"
	"github.com/bmeg/grip/gripql"
)

func TestSchemaScanner(t *testing.T) {
	graph := "example-graph"
	conf := config.DefaultConfig()
	config.TestifyConfig(conf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := example.StartTestServer(ctx, conf, graph)
	if err != nil {
		t.Fatal(err)
	}

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
		case "friend", "pilots", "appearsIn":
		default:
			t.Log(v)
			t.Errorf("Unexpected type %s ", v.Label)
		}
	}

	sort.Sort(gripql.EdgeSorter(example.SWSchema.Edges))
	sort.Sort(gripql.EdgeSorter(graphSchema.Edges))
	if !reflect.DeepEqual(example.SWSchema.Edges, graphSchema.Edges) {
		t.Logf("expected: %+v", example.SWSchema.Edges)
		t.Logf("actual:   %+v", graphSchema.Edges)
		t.Fatal("unexpected edge schemas")
	}

	sort.Sort(gripql.VertexSorter(example.SWSchema.Vertices))
	sort.Sort(gripql.VertexSorter(graphSchema.Vertices))
	if !reflect.DeepEqual(example.SWSchema.Vertices, graphSchema.Vertices) {
		t.Logf("expected: %+v", example.SWSchema.Vertices)
		t.Logf("actual:   %+v", graphSchema.Vertices)
		t.Fatal("unexpected vertex schemas")
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
		case "appearsIn", "friend", "pilots":
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
		case "friend", "pilots":
		default:
			t.Errorf("Unexpected type %s", v.Label)
		}
	}
}
