package benchmark

import (
	"math/rand"
	"os"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
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
	kv, _ := badgerdb.NewKVInterface("test_1.db", kvi.Options{})
	graphDB := kvgraph.NewKVGraph(kv)
	graphDB.AddGraph("test")
	graph, err := graphDB.Graph("test")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := make([]*gdbi.Vertex, 1000)
		for j := 0; j < 1000; j++ {
			v[j] = &gdbi.Vertex{ID: randID(), Label: "Person"}
		}
		graph.AddVertex(v)
	}
	b.StopTimer()

	graphDB.Close()
	os.RemoveAll("test_1.db")
}

func BenchmarkEdgeInsert(b *testing.B) {
	kv, _ := badgerdb.NewKVInterface("test_1.db", kvi.Options{})
	graphDB := kvgraph.NewKVGraph(kv)
	graphDB.AddGraph("test")
	graph, err := graphDB.Graph("test")
	if err != nil {
		b.Fatal(err)
	}

	gids := make([]string, 1000)
	v := make([]*gdbi.Vertex, 1000)
	for j := 0; j < 1000; j++ {
		gids[j] = randID()
		v[j] = &gdbi.Vertex{ID: gids[j], Label: "Person"}
	}
	graph.AddVertex(v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := make([]*gdbi.Edge, 1000)
		for j := 0; j < 1000; j++ {
			src := gids[rand.Intn(len(gids))]
			dst := gids[rand.Intn(len(gids))]
			e[j] = &gdbi.Edge{From: src, To: dst, Label: "friend"}
		}
		graph.AddEdge(e)
	}
	b.StopTimer()

	graphDB.Close()
	os.RemoveAll("test_1.db")
}
