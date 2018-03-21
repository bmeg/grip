package engine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/kvgraph"
	"testing"
)

// Dead simple baseline tests: get all vertices from a memory-backed graph.
func BenchmarkBaselineV(b *testing.B) {
	kv, _ := badgerdb.BadgerBuilder("test-badger.db")
	db := kvgraph.NewKVGraph(kv).Graph("test-graph")

	for i := 0; i < 1000; i++ {
		gid := fmt.Sprintf("v-%d", i)
		db.AddVertex([]*aql.Vertex{{Gid: gid, Label: "Vert"}})
	}

	q := aql.V()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p, err := Compile(q.Statements, db, "workDir")
		if err != nil {
			b.Fatal(err)
		}
		o := p.Run()
		for range o {
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}
