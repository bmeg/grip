package benchmark

import (
	"math/rand"
	"os"
	"testing"

	"github.com/bmeg/arachne/aql"
	//"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/kvgraph"
)

var idRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}

func BenchmarkVertexInsert(b *testing.B) {
	kv, _ := badgerdb.NewKVInterface("test_1.db")
	graphDB := kvgraph.NewKVGraph(kv)
	graphDB.AddGraph("test")
	graph, err := graphDB.Graph("test")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := make([]*aql.Vertex, 1000)
		for j := 0; j < 1000; j++ {
			v[j] = &aql.Vertex{Gid: randID(), Label: "Person"}
		}
		graph.AddVertex(v)
	}
	b.StopTimer()

	graphDB.Close()
	os.RemoveAll("test_1.db")
}

func BenchmarkEdgeInsert(b *testing.B) {
	kv, _ := badgerdb.NewKVInterface("test_1.db")
	graphDB := kvgraph.NewKVGraph(kv)
	graphDB.AddGraph("test")
	graph, err := graphDB.Graph("test")
	if err != nil {
		b.Fatal(err)
	}

	gids := make([]string, 1000)
	v := make([]*aql.Vertex, 1000)
	for j := 0; j < 1000; j++ {
		gids[j] = randID()
		v[j] = &aql.Vertex{Gid: gids[j], Label: "Person"}
	}
	graph.AddVertex(v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := make([]*aql.Edge, 1000)
		for j := 0; j < 1000; j++ {
			src := gids[rand.Intn(len(gids))]
			dst := gids[rand.Intn(len(gids))]
			e[j] = &aql.Edge{From: src, To: dst, Label: "friend"}
		}
		graph.AddEdge(e)
	}
	b.StopTimer()

	graphDB.Close()
	os.RemoveAll("test_1.db")
}
