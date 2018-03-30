package test

import (
	"github.com/bmeg/arachne/aql"
	"math/rand"
	"os"
	"testing"
	//"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/kvgraph"
)

var idRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randId() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}

func BenchmarkVertexInsert(b *testing.B) {
	kv, _ := badgerdb.BadgerBuilder("test_1.db")
	graphDB := kvgraph.NewKVGraph(kv)
	graphDB.AddGraph("test")
	graph := graphDB.Graph("test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := make([]*aql.Vertex, 1000)
		for j := 0; j < 1000; j++ {
			v[j] = &aql.Vertex{Gid: randId(), Label: "Person"}
		}
		graph.AddVertex(v)
	}
	b.StopTimer()

	graphDB.Close()
	os.RemoveAll("test_1.db")
}
