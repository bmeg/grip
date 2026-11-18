package table_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper/node"
	"github.com/bmeg/grip/gripper/node/nodetest"
	"github.com/bmeg/grip/gripper/node/table"
)

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadMapping(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "characters.csv", "id,name,height,homeworld\n1,Luke,172,1\n2,C-3PO,167,\n")
	write(t, dir, "films.jsonl", `{"id": "1", "title": "A New Hope"}`+"\n")
	write(t, dir, "map.yaml", `
vertices:
  - file: characters.csv
    label: Character
    prefix: "Character:"
    id: id
edges:
  - file: characters.csv
    label: homeworld
    from: {field: id, prefix: "Character:"}
    to: {field: homeworld, prefix: "Planet:"}
`)
	m, err := table.LoadMapping(filepath.Join(dir, "map.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	g := nodetest.NewGraph(t, "chars")
	nv, ne, err := table.Load(context.Background(), g, m, dir, "Character:")
	if err != nil {
		t.Fatal(err)
	}
	if nv != 2 || ne != 1 {
		t.Fatalf("loaded %d vertices and %d edges", nv, ne)
	}
	v := g.GetVertex("Character:1", true)
	if v == nil || v.Label != "Character" || v.Data["height"] != float64(172) || v.Data["name"] != "Luke" {
		t.Errorf("vertex %#v", v)
	}
	e := g.GetEdge("Character:1-homeworld-Planet:1", true)
	if e == nil || e.From != "Character:1" || e.To != "Planet:1" {
		t.Errorf("edge %#v", e)
	}

	// the loaded graph advertises its cross-prefix edge
	links, err := node.ScanEdgeLinks(context.Background(), g, "Character:")
	if err != nil || len(links) != 1 || links[0].ToPrefix != "Planet:" || links[0].Label != "homeworld" {
		t.Errorf("links %v %v", links, err)
	}

	// JSON lines tables work too
	m2 := &table.Mapping{Vertices: []table.VertexTable{{File: "films.jsonl", Label: "Film", Prefix: "Film:", ID: "id"}}}
	g2 := nodetest.NewGraph(t, "films")
	if nv, _, err := table.Load(context.Background(), g2, m2, dir, "Film:"); err != nil || nv != 1 {
		t.Errorf("jsonl load: %d %v", nv, err)
	}
}

func TestPrefixViolations(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "rows.csv", "id,other\n1,2\n")
	g := nodetest.NewGraph(t, "x")
	cases := map[string]*table.Mapping{
		"vertex": {Vertices: []table.VertexTable{{File: "rows.csv", Label: "X", Prefix: "Other:", ID: "id"}}},
		"edge": {Edges: []table.EdgeTable{{File: "rows.csv", Label: "l",
			From: table.Endpoint{Field: "other", Prefix: "Foreign:"}, To: table.Endpoint{Field: "id", Prefix: "Node:"}}}},
	}
	for name, m := range cases {
		_, _, err := table.Load(context.Background(), g, m, dir, "Node:")
		if err == nil || !strings.Contains(err.Error(), "node prefix") {
			t.Errorf("%s: expected prefix error, got %v", name, err)
		}
	}
	var _ gdbi.GraphInterface = g
}
