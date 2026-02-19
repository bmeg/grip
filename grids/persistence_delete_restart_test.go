package grids

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/key"
)

func bulkAddElems(t *testing.T, g *Graph, elems []*gdbi.GraphElement) {
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

func countPosKeysForID(t *testing.T, kv *pebblebulk.PebbleKV, id string) int {
	t.Helper()
	count := 0
	err := kv.View(func(it *pebblebulk.PebbleIterator) error {
		prefix := []byte{benchtop.PosPrefix}
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			_, rowID := benchtop.ParsePosKey(it.Key())
			if string(rowID) == id {
				count++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("countPosKeysForID iterator error: %v", err)
	}
	return count
}

func listPosKeysForID(t *testing.T, kv *pebblebulk.PebbleKV, id string) []string {
	t.Helper()
	out := []string{}
	err := kv.View(func(it *pebblebulk.PebbleIterator) error {
		prefix := []byte{benchtop.PosPrefix}
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
			tid, rowID := benchtop.ParsePosKey(it.Key())
			if string(rowID) != id {
				continue
			}
			val, err := it.Value()
			if err != nil {
				out = append(out, "value_error")
				continue
			}
			loc := benchtop.DecodeRowLoc(val)
			if loc == nil {
				out = append(out, "tid="+strconv.Itoa(int(tid))+" loc=nil")
				continue
			}
			out = append(out, "tid="+
				strconv.Itoa(int(tid))+
				" loc.table="+strconv.Itoa(int(loc.TableId)))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("listPosKeysForID iterator error: %v", err)
	}
	return out
}

func openGraphForTest(t *testing.T, conf Config, graph string) (*GDB, *Graph) {
	t.Helper()
	dbi, err := NewGraphDB(conf)
	if err != nil {
		t.Fatalf("NewGraphDB failed: %v", err)
	}
	gi, err := dbi.Graph(graph)
	if err != nil {
		dbi.Close()
		t.Fatalf("Graph(%s) failed: %v", graph, err)
	}
	return dbi.(*GDB), gi.(*Graph)
}

func TestDeletePersistsAcrossRestart(t *testing.T) {
	conf := Config{
		GraphDir: t.TempDir(),
		Driver:   "jsontable",
	}
	const graphName = "g"

	// First run: create graph and load data.
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

	elems := make([]*gdbi.GraphElement, 0, 10)
	for i := 0; i < 10; i++ {
		id := "obs:" + string(rune('a'+i))
		elems = append(elems, &gdbi.GraphElement{
			Vertex: &gdbi.Vertex{
				ID:    id,
				Label: "Observation",
				Data: map[string]any{
					"status": "final",
					"n":      i,
				},
			},
		})
	}
	bulkAddElems(t, g, elems)
	dbi.Close()

	// Restart, then delete a subset.
	dbi2, g2 := openGraphForTest(t, conf, graphName)
	for _, id := range []string{"obs:a", "obs:b", "obs:c", "obs:d", "obs:e"} {
		var found bool
		var label string
		err := g2.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			vk := key.VertexKey(id)
			if err := it.Seek(vk); err != nil {
				return err
			}
			if it.Valid() && bytes.Equal(it.Key(), vk) {
				v, err := it.Value()
				if err != nil {
					return err
				}
				found = true
				label = string(v)
			}
			return nil
		})
		if err != nil {
			dbi2.Close()
			t.Fatalf("pre-delete vertex key check failed for %s: %v", id, err)
		}
		if !found || label == "" {
			dbi2.Close()
			t.Fatalf("pre-delete vertex key missing/empty for %s: found=%v label=%q", id, found, label)
		}
	}
	del := &gdbi.DeleteData{
		Graph:    graphName,
		Vertices: []string{"obs:a", "obs:b", "obs:c", "obs:d", "obs:e"},
	}
	if err := g2.BulkDel(del); err != nil {
		dbi2.Close()
		t.Fatalf("BulkDel failed: %v", err)
	}
	for _, id := range del.Vertices {
		if c := countPosKeysForID(t, g2.driver.Pkv, id); c != 0 {
			t.Fatalf("post-delete pre-restart expected 0 pos keys for %s, got %d (%v)", id, c, listPosKeysForID(t, g2.driver.Pkv, id))
		}
	}
	dbi2.Close()

	// Restart again and verify both logical and persisted location state.
	dbi3, g3 := openGraphForTest(t, conf, graphName)
	defer dbi3.Close()

	// Deleted IDs should not resolve or exist as vertices.
	for _, id := range []string{"obs:a", "obs:b", "obs:c", "obs:d", "obs:e"} {
		if v := g3.GetVertex(id, false); v != nil {
			t.Fatalf("expected deleted vertex %s to be absent, got %#v", id, v)
		}
	}

	// Non-deleted IDs should still exist.
	for _, id := range []string{"obs:f", "obs:g", "obs:h", "obs:i", "obs:j"} {
		if v := g3.GetVertex(id, false); v == nil {
			t.Fatalf("expected surviving vertex %s to exist", id)
		}
	}

}
