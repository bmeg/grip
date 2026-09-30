package grids

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bmeg/grip/gdbi"
)

// TestIssueRepro attempts to reproduce the issue where hasLabel returns duplicates after restart.
func TestIssueRepro(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	const graphName = "g"
	const label = "Observation"
	const numVertices = 100

	// Helper to load data
	loadData := func(t *testing.T, g *Graph, start int, count int) {
		t.Helper()
		elems := make([]*gdbi.GraphElement, 0, count)
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("obs:%d", start+i)
			elems = append(elems, &gdbi.GraphElement{
				Vertex: &gdbi.Vertex{
					ID:    id,
					Label: label,
					Data: map[string]any{
						"status": "final",
						"n":      start + i,
					},
				},
			})
		}
		if err := g.BulkAdd(asChan(elems)); err != nil {
			t.Fatalf("BulkAdd failed: %v", err)
		}
	}

	// 1. First run: create graph and load data.
	t.Log("--- Run 1 ---")
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	loadData(t, g, 0, numVertices)

	// Query 1
	count1 := countLabel(t, g, label)
	t.Logf("Run 1 Count: %d", count1)
	if count1 != numVertices {
		t.Errorf("Run 1: expected %d, got %d", numVertices, count1)
	}

	// Delete some data (mimic user script)
	// Deleting first 50
	toDelete := []string{}
	for i := 0; i < 50; i++ {
		toDelete = append(toDelete, fmt.Sprintf("obs:%d", i))
	}
	delData := &gdbi.DeleteData{
		Graph:    graphName,
		Vertices: toDelete,
	}
	if err := g.BulkDel(delData); err != nil {
		t.Fatalf("BulkDel failed: %v", err)
	}

	count1b := countLabel(t, g, label)
	t.Logf("Run 1 Post-Delete Count: %d", count1b)
	if count1b != 50 {
		t.Errorf("Run 1 Post-Delete: expected 50, got %d", count1b)
	}

	// Reload data (mimic user script)
	// Reloading same 100 vertices
	loadData(t, g, 0, numVertices)

	count1c := countLabel(t, g, label)
	t.Logf("Run 1 Post-Reload Count: %d", count1c)
	if count1c != numVertices {
		t.Errorf("Run 1 Post-Reload: expected %d, got %d", numVertices, count1c)
	}

	dbi.Close()

	// 2. Restart and Query
	t.Log("--- Run 2 (Restart) ---")
	time.Sleep(100 * time.Millisecond) // Give it a moment

	dbi2, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB restart failed: %v", err)
	}
	gi2, err := dbi2.Graph(graphName)
	if err != nil {
		t.Fatalf("Graph restart failed: %v", err)
	}
	g2 := gi2.(*Graph)

	count2 := countLabel(t, g2, label)
	t.Logf("Run 2 Count: %d", count2)

	// Also check for duplicates specifically
	ids := getIDs(t, g2, label)
	if hasDuplicates(ids) {
		t.Errorf("Run 2 found duplicates in result IDs!")
		dumpDuplicates(t, ids)
	}

	if count2 != numVertices {
		t.Fatalf("Run 2: expected %d, got %d. (Duplicates detected?)", numVertices, count2)
	}

	dbi2.Close()
	os.RemoveAll(conf.GraphDir)
}

func asChan(elems []*gdbi.GraphElement) chan *gdbi.GraphElement {
	ch := make(chan *gdbi.GraphElement, len(elems))
	for _, e := range elems {
		ch <- e
	}
	close(ch)
	return ch
}

func countLabel(t *testing.T, g *Graph, label string) int {
	ctx := context.Background()
	// Using VertexLabelScan via driver logic, which is what V().hasLabel() does

	// We can call VertexLabelScan directly
	scanChan := g.VertexLabelScan(ctx, label)
	count := 0
	for range scanChan {
		count++
	}
	return count
}

func getIDs(t *testing.T, g *Graph, label string) []string {
	ctx := context.Background()
	scanChan := g.VertexLabelScan(ctx, label)
	out := []string{}
	for id := range scanChan {
		out = append(out, id)
	}
	return out
}

func hasDuplicates(ids []string) bool {
	seen := make(map[string]struct{})
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func dumpDuplicates(t *testing.T, ids []string) {
	seen := make(map[string]int)
	for _, id := range ids {
		seen[id]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Logf("Duplicate: %s appears %d times", id, count)
		}
	}
}
