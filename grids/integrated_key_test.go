package grids

import (
	"fmt"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/key"
)

func TestIntegratedKeyRowLoc(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	defer fmt.Println("Done")

	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("failed to create dbi: %v", err)
	}
	defer dbi.Close()

	if err := dbi.AddGraph("test"); err != nil {
		t.Fatalf("failed to add graph: %v", err)
	}

	gi, _ := dbi.Graph("test")
	g := gi.(*Graph)

	// Add a vertex
	vID := "v1"
	vLabel := "Person"
	err = g.AddVertex([]*gdbi.Vertex{
		{
			ID:    vID,
			Label: vLabel,
			Data: map[string]any{
				"name": "Alice",
			},
		},
	})
	if err != nil {
		t.Fatalf("AddVertex failed: %v", err)
	}

	// Verify the key in Pebble directly
	vkey := key.VertexKey(vID)
	err = g.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		val, err := it.Get(vkey)
		if err != nil {
			return err
		}
		label, loc := benchtop.DecodeVertexValue(val)
		if label != vLabel {
			t.Errorf("expected label %s, got %s", vLabel, label)
		}
		if loc == nil {
			t.Errorf("RowLoc should not be nil in integrated vertex key")
		} else {
			t.Logf("Found RowLoc in key: %+v", loc)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Pebble View error: %v", err)
	}

	// Add an edge
	eID := "e1"
	eLabel := "knows"
	err = g.AddEdge([]*gdbi.Edge{
		{
			ID:    eID,
			From:  vID,
			To:    vID,
			Label: eLabel,
			Data: map[string]any{
				"since": 2020,
			},
		},
	})
	if err != nil {
		t.Fatalf("AddEdge failed: %v", err)
	}

	// Verify edge key
	ekey := key.EdgeKey(eID, vID, vID, eLabel)
	err = g.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		val, err := it.Get(ekey)
		if err != nil {
			return err
		}
		_, loc := benchtop.DecodeEdgeValue(val)
		if loc == nil {
			t.Errorf("RowLoc should not be nil in integrated edge value")
		} else {
			t.Logf("Found RowLoc in edge value: %+v", loc)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Pebble View error: %v", err)
	}
}
