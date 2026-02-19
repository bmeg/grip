package grids

import (
	"testing"

	"github.com/bmeg/grip/gdbi"
)

func runBulkAdd(t *testing.T, g *Graph, elems ...*gdbi.GraphElement) {
	t.Helper()
	ch := make(chan *gdbi.GraphElement, len(elems))
	for _, e := range elems {
		ch <- e
	}
	close(ch)
	if err := g.BulkAdd(ch); err != nil {
		t.Fatalf("BulkAdd failed: %v", err)
	}
}

func TestBulkAddSkipsExistingVertexIDs(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	defer dbi.Close()

	if err := dbi.AddGraph("g"); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph("g")
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	// First insert.
	runBulkAdd(t, g, &gdbi.GraphElement{
		Vertex: &gdbi.Vertex{
			ID:    "v1",
			Label: "person",
			Data:  map[string]any{"name": "first"},
		},
	})

	// Second insert with same ID should be skipped (insert-only semantics).
	runBulkAdd(t, g, &gdbi.GraphElement{
		Vertex: &gdbi.Vertex{
			ID:    "v1",
			Label: "person",
			Data:  map[string]any{"name": "second"},
		},
	})

	v := g.GetVertex("v1", true)
	if v == nil {
		t.Fatalf("expected vertex v1 to exist")
	}
	gotName, _ := v.Data["name"].(string)
	if gotName != "first" {
		t.Fatalf("expected duplicate insert to be skipped; name=%q want=%q", gotName, "first")
	}

	table, err := g.driver.GetOrLoadTable("v_person")
	if err != nil {
		t.Fatalf("getOrLoadTable(v_person) failed: %v", err)
	}
	count := 0
	for range table.ScanId(nil) {
		count++
	}
	if count != 1 {
		t.Fatalf("expected exactly one physical row for duplicate ID; got=%d want=1", count)
	}
}
