package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/badgerdb"
	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/kvgraph"
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
		db.AddVertex([]*gripql.Vertex{{Gid: gid, Label: "Vert"}})
	}

	q := gripql.V()

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
