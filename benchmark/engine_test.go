package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/kvgraph"
)

// Dead simple baseline tests: get all vertices from a memory-backed graph.
func BenchmarkBaselineV(b *testing.B) {
	kv, _ := badgerdb.NewKVInterface("test-badger.db")
	db, err := kvgraph.NewKVGraph(kv).Graph("test-graph")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		gid := fmt.Sprintf("v-%d", i)
		db.AddVertex([]*aql.Vertex{{Gid: gid, Label: "Vert"}})
	}

	q := aql.V()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p, err := db.Compiler().Compile(q.Statements)
		if err != nil {
			b.Fatal(err)
		}
		o := engine.Run(context.Background(), p, "workDir")
		for range o {
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}
