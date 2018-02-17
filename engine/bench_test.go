package engine

import (
  "context"
  "fmt"
  "testing"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/memgraph"
)

// Dead simple baseline tests: get all vertices from a memory-backed graph.
func BenchmarkBaselineV(b *testing.B) {
	ctx := context.Background()
  db := memgraph.NewMemGraph()

  for i := 0; i < 1000; i++ {
    gid := fmt.Sprintf("v-%d", i)
    db.AddVertex(&aql.Vertex{Gid: gid, Label: "Vert"})
  }

  q := aql.V()

  b.ResetTimer()

  for i := 0; i < b.N; i++ {
    _, err := Run(ctx, q.Statements, db)
    if err != nil {
      b.Fatal(err)
    }
  }
}
