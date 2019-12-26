package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
)

// Dead simple baseline tests: get all vertices from a memory-backed graph.
func BenchmarkBaselineV(b *testing.B) {
	kv, _ := badgerdb.NewKVInterface("test-badger.db", kvi.Options{})
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
		o := pipeline.Run(context.Background(), p, "workDir")
		for range o {
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}
