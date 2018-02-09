package tests

import (
  "context"
  "fmt"
  "testing"
	_ "github.com/bmeg/arachne/boltdb" // import so bolt will register itself
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/graphserver"
)

func BenchmarkBoltWrite(b *testing.B) {
	a, err := kvgraph.NewKVArachne("bolt", "./testingdir")
	if err != nil {
    b.Fatal(err)
	}

  ctx := context.Background()
  eng := graphserver.NewGraphEngine(a)
  WriteTestData(1000, &eng)
  q := aql.NewQuery("testgraph").V().HasLabel("Foo")
  eng.RunTraversal(ctx, q.GraphQuery)
}

func BenchmarkBoltQuery1(b *testing.B) {
	a, err := kvgraph.NewKVArachne("bolt", "./testingdir")
	if err != nil {
    b.Fatal(err)
	}

  ctx := context.Background()
  eng := graphserver.NewGraphEngine(a)
  WriteTestData(1000, &eng)
  b.ResetTimer()

  q := aql.NewQuery("testgraph").V().HasLabel("Foo")
  eng.RunTraversal(ctx, q.GraphQuery)
}

func WriteTestData(n int, eng *graphserver.GraphEngine) {
  for i := 0; i < n; i++ {
    eng.AddVertex("testgraph", aql.Vertex{
      Gid: fmt.Sprintf("foo-%d", i),
      Label: "Foo",
    })

    eng.AddVertex("testgraph", aql.Vertex{
      Gid: fmt.Sprintf("bar-%d", i),
      Label: "Bar",
    })

    eng.AddVertex("testgraph", aql.Vertex{
      Gid: fmt.Sprintf("baz-%d", i),
      Label: "Baz",
    })

    eng.AddEdge("testgraph", aql.Edge{
      Gid: fmt.Sprintf("foo-bar-%d", i),
      Label: "foo-bar",
      From: fmt.Sprintf("foo-%d", i),
      To: fmt.Sprintf("bar-%d", i),
    })

    eng.AddEdge("testgraph", aql.Edge{
      Gid: fmt.Sprintf("bar-baz-%d", i),
      Label: "bar-baz",
      From: fmt.Sprintf("bar-%d", i),
      To: fmt.Sprintf("baz-%d", i),
    })
  }
}
