package table_test

import (
	"context"
	"errors"
	"testing"

	griptable "github.com/bmeg/grip/table"
)

func tableGraphConfig() griptable.Config {
	return griptable.Config{
		Graphs: []griptable.GraphConfig{{
			Graph: "standalone",
			Mapping: griptable.Mapping{
				Vertices: []griptable.VertexTable{{
					Collection: "people",
					Label:      "Person",
					Prefix:     "Person:",
					ID:         "id",
				}},
			},
		}},
	}
}

func TestTableGraphDB(t *testing.T) {
	driver := &graphFakeTable{
		collections: map[string]griptable.Collection{
			"people": {
				Name: "people", PrimaryKey: "id",
				Fields: []griptable.Field{{Name: "id", Indexed: true}, {Name: "name"}},
			},
		},
		rows: map[string][]*griptable.Row{
			"people": {{ID: "1", Data: map[string]interface{}{"id": "1", "name": "Luke"}}},
		},
	}

	db, err := griptable.NewGraphDB(context.Background(), driver, tableGraphConfig())
	if err != nil {
		t.Fatal(err)
	}
	if graphs := db.ListGraphs(); len(graphs) != 1 || graphs[0] != "standalone" {
		t.Fatalf("ListGraphs() = %v", graphs)
	}
	graph, err := db.Graph("standalone")
	if err != nil {
		t.Fatal(err)
	}
	vertex := graph.GetVertex("Person:1", true)
	if vertex == nil || vertex.Data["name"] != "Luke" {
		t.Fatalf("table graph vertex = %#v", vertex)
	}
	if err := db.DeleteGraph("standalone"); !errors.Is(err, griptable.ErrUnsupported) {
		t.Fatalf("DeleteGraph() = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConfiguredTableGraphDB(t *testing.T) {
	name := "test-configured-table"
	err := griptable.RegisterFactory(name, func(context.Context, map[string]interface{}) (griptable.TableDriver, error) {
		return &graphFakeTable{
			collections: map[string]griptable.Collection{
				"people": {Name: "people", PrimaryKey: "id", Fields: []griptable.Field{{Name: "id"}}},
			},
			rows: map[string][]*griptable.Row{
				"people": {{ID: "1", Data: map[string]interface{}{"id": "1", "name": "Luke"}}},
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	config := tableGraphConfig()
	config.Driver = name
	db, err := griptable.NewConfiguredGraphDB(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(db.ListGraphs()) != 1 {
		t.Fatalf("ListGraphs() = %v", db.ListGraphs())
	}
}

func TestTableGraphDBValidation(t *testing.T) {
	driver := &graphFakeTable{
		collections: map[string]griptable.Collection{
			"people": {Name: "people", PrimaryKey: "id", Fields: []griptable.Field{{Name: "id"}}},
		},
		rows: map[string][]*griptable.Row{
			"people": {{ID: "1", Data: map[string]interface{}{"id": "1"}}},
		},
	}
	config := tableGraphConfig()
	config.Graphs = append(config.Graphs, config.Graphs[0])
	if _, err := griptable.NewGraphDB(context.Background(), driver, config); !errors.Is(err, griptable.ErrInvalidRequest) {
		t.Fatalf("NewGraphDB duplicate graph = %v", err)
	}

	config = tableGraphConfig()
	config.Graphs[0].Graph = "valid"
	if _, err := griptable.NewGraphDB(context.Background(), nil, config); !errors.Is(err, griptable.ErrInvalidRequest) {
		t.Fatalf("NewGraphDB nil driver = %v", err)
	}
}
