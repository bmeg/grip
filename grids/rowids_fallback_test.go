package grids

import (
	"testing"

	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/gdbi"
)

func TestRowIdsByLabelsFieldValueFallsBackWhenUnindexed(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	defer dbi.Close()

	const graphName = "g"
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	elems := []*gdbi.GraphElement{
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:1",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/test",
				},
			},
		},
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:2",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/testtwo",
				},
			},
		},
	}
	ch := make(chan *gdbi.GraphElement, len(elems))
	for _, e := range elems {
		ch <- e
	}
	close(ch)
	if err := g.BulkAdd(ch); err != nil {
		t.Fatalf("BulkAdd failed: %v", err)
	}

	// Do not add index; this should use fallback scan path and still match.
	count := 0
	for range g.driver.RowIdsByLabelsFieldValue(
		[]string{"Observation"},
		"auth_resource_path",
		"/programs/calypr/projects/test",
		query.EQ,
	) {
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 unindexed fallback match, got %d", count)
	}
}

func TestRowIdsByLabelsFieldValueFallbackSkipsDeletedTombstones(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	defer dbi.Close()

	const graphName = "g"
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	elems := []*gdbi.GraphElement{
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:1",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/test",
				},
			},
		},
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:2",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/test",
				},
			},
		},
	}
	ch := make(chan *gdbi.GraphElement, len(elems))
	for _, e := range elems {
		ch <- e
	}
	close(ch)
	if err := g.BulkAdd(ch); err != nil {
		t.Fatalf("BulkAdd failed: %v", err)
	}

	if err := g.BulkDel(&gdbi.DeleteData{
		Graph:    graphName,
		Vertices: []string{"obs:1"},
	}); err != nil {
		t.Fatalf("BulkDel failed: %v", err)
	}

	count := 0
	for range g.driver.RowIdsByLabelsFieldValue(
		[]string{"Observation"},
		"auth_resource_path",
		"/programs/calypr/projects/test",
		query.EQ,
	) {
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 live match after delete, got %d", count)
	}
}

func TestRowIdsByLabelsFieldValueWithinFallbackSkipsDeletedTombstones(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	defer dbi.Close()

	const graphName = "g"
	if err := dbi.AddGraph(graphName); err != nil {
		t.Fatalf("AddGraph failed: %v", err)
	}
	gi, err := dbi.Graph(graphName)
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	g := gi.(*Graph)

	elems := []*gdbi.GraphElement{
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:1",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/test",
				},
			},
		},
		{
			Vertex: &gdbi.Vertex{
				ID:    "obs:2",
				Label: "Observation",
				Data: map[string]any{
					"auth_resource_path": "/programs/calypr/projects/testtwo",
				},
			},
		},
	}
	ch := make(chan *gdbi.GraphElement, len(elems))
	for _, e := range elems {
		ch <- e
	}
	close(ch)
	if err := g.BulkAdd(ch); err != nil {
		t.Fatalf("BulkAdd failed: %v", err)
	}

	if err := g.BulkDel(&gdbi.DeleteData{
		Graph:    graphName,
		Vertices: []string{"obs:1"},
	}); err != nil {
		t.Fatalf("BulkDel failed: %v", err)
	}

	count := 0
	for range g.driver.RowIdsByLabelsFieldValue(
		[]string{"Observation"},
		"auth_resource_path",
		[]any{"/programs/calypr/projects/test", "/programs/calypr/projects/testtwo"},
		query.WITHIN,
	) {
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 live within match after delete, got %d", count)
	}
}
