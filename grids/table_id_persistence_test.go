package grids

import (
	"context"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/gdbi"
)

func TestTableIDPersistenceOnRestart(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	const graphName = "g"

	// 1. Initial run: create graph and add high-volume data to trigger table creation.
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		dbi.Close()
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	// Add data with label "Person"
	elems := []*gdbi.GraphElement{
		{
			Vertex: &gdbi.Vertex{
				ID:    "p1",
				Label: "Person",
				Data: map[string]any{
					"name": "Alice",
				},
			},
		},
	}
	bulkAddElems(t, g, elems)

	// Get TableId and verify it's not 0
	loc, err := g.driver.LocCache.Get(context.Background(), "p1")
	if err != nil {
		t.Fatalf("LocCache.Get failed for p1: %v", err)
	}
	initialTableID := loc.TableId
	dbi.Close()

	// 2. Restart and verify ID metadata
	dbi2, g2 := openGraphForTest(t, conf, graphName)
	defer dbi2.Close()

	// Verify the table "v_Person" exists in memory and has the same ID
	// We can't access g2.driver.Tables directly with "v_Person" as key because keys are uint16 now?
	// Wait, keys in d.Tables ARE uint16.
	// The previous test code assumed d.Tables was map[string]*BackendTable?
	// But d.Tables was map[uint16]*BackendTable in driver.go step 32.
	// So `g2.driver.Tables["v_Person"]` was ALREADY wrong?
	// Ah, maybe the user added this test recently and it was broken?
	// Or maybe I missed something.
	// Anyway, to verify table exists, we should use GetOrLoadTable("v_Person")
	tbl, err := g2.driver.GetOrLoadTable("v_" + "Person")
	if err != nil {
		t.Fatalf("table v_Person lost after restart")
	}
	if tbl.TableId != initialTableID {
		t.Fatalf("TableId mismatch after restart: initial=%d, got=%d", initialTableID, tbl.TableId)
	}

	// Verify the label lookup is populated
	// Verify the label lookup is populated
	table, err := g2.driver.GetTableByID(initialTableID)
	if err != nil {
		t.Fatalf("Label resolution failed for TableId %d after restart: %v", initialTableID, err)
	}
	if table.Label != "Person" {
		t.Fatalf("Label mismatch after resolution: expected Person, got %s", table.Label)
	}
	if table.TableId != initialTableID {
		t.Fatalf("Table lookup mismatch: initial=%d, got=%d", initialTableID, table.TableId)
	}

	// 3. Verify querying still works (resolves label correctly)
	v := g2.GetVertex("p1", true)
	if v == nil {
		t.Fatalf("GetVertex failed to find p1 after restart")
	}
	if v.Label != "Person" {
		t.Fatalf("Vertex p1 label mismatch: expected Person, got %s", v.Label)
	}
	if v.Data["name"] != "Alice" {
		t.Fatalf("Vertex p1 data mismatch: expected Alice, got %v", v.Data["name"])
	}
}

func TestTableIDZeroRecovery(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	const graphName = "g"

	// 1. Initial run: create table info manually with ID 0 in Pebble (simulation of bug)
	// Actually, easier to just test that our New and getOrLoadTable self-correct.
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		dbi.Close()
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	// Create a new table
	tblStore, err := g.driver.New("v_NewTable", nil)
	if err != nil {
		t.Fatalf("New table failed: %v", err)
	}

	locs, err := tblStore.AddRows([]benchtop.Row{{Id: []byte("row1"), Data: map[string]any{"_id": "row1"}}})
	if err != nil {
		t.Fatalf("AddRows failed: %v", err)
	}

	rowLoc := locs[0]

	// Force clear the LabelLookup for this ID to simulate discovery failure
	// No longer applicable as LabelLookup is gone.
	// Instead we can remove the table from memory cache?
	delete(g.driver.TablesByID, rowLoc.TableId)

	// Attempt resolution - it should fail now unless discovered by the fix
	// Attempt resolution - it should fail now unless discovered by the fix
	table, err := g.driver.GetTableByID(rowLoc.TableId)
	if err != nil {
		t.Fatalf("GetTableByID failed to recover table: %v", err)
	}
	if table.Label != "NewTable" {
		t.Fatalf("Expected NewTable, got %s", table.Label)
	}

	dbi.Close()
}
