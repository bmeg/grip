package server_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bmeg/grip/config"
	gripserver "github.com/bmeg/grip/server"
	griptable "github.com/bmeg/grip/table"
	"github.com/bmeg/grip/table/filetable"
)

func TestStartTableDriver(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "people.jsonl"),
		[]byte(`{"id":"1","name":"Luke"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	name := "server-filetable"
	err := griptable.RegisterFactory(name, func(ctx context.Context, config map[string]interface{}) (griptable.TableDriver, error) {
		path, _ := config["dir"].(string)
		return filetable.New(path)
	})
	if err != nil {
		t.Fatal(err)
	}

	db, err := gripserver.StartDriver(config.DriverConfig{
		Table: &griptable.Config{
			Driver: name,
			Config: map[string]interface{}{"dir": dir},
			Graphs: []griptable.GraphConfig{{
				Graph: "standalone",
				Mapping: griptable.Mapping{
					Vertices: []griptable.VertexTable{{
						Collection: "people.jsonl",
						Label:      "Person",
						Prefix:     "Person:",
						ID:         "id",
					}},
				},
			}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	graphs := db.ListGraphs()
	if len(graphs) != 1 || graphs[0] != "standalone" {
		t.Fatalf("ListGraphs() = %v", graphs)
	}
	graph, err := db.Graph("standalone")
	if err != nil {
		t.Fatal(err)
	}
	if vertex := graph.GetVertex("Person:1", true); vertex == nil || vertex.Data["name"] != "Luke" {
		t.Fatalf("GetVertex() = %#v", vertex)
	}
}
