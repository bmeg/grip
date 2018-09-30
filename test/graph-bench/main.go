package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/protoutil"

	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	_ "github.com/bmeg/grip/kvi/rocksdb"  // import so rocks will register itself
)

var idRunes = []rune("abcdefghijklmnopqrstuvwxyz")
var labelValues = []string{
	"Person",
	"Place",
	"Thing",
}
var fieldNames = []string{
	"firstName",
	"lastName",
	"city",
	"state",
}

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}

func randLabel() string {
	return labelValues[rand.Intn(len(labelValues))]
}

func randData() map[string]interface{} {
	o := map[string]interface{}{}
	for _, i := range fieldNames {
		o[i] = randID()
	}
	return o
}

func randVertex() *gripql.Vertex {
	g := gripql.Vertex{
		Gid:   randID(),
		Label: randLabel(),
		Data:  protoutil.AsStruct(randData()),
	}
	return &g
}

func logBenchmark(f func()) {
	start := time.Now()
	f()
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Time: %s", elapsed)
}

func randomVertexInsert(kgraph gdbi.GraphInterface) {
	for i := 0; i < 10000; i++ {
		d := []*gripql.Vertex{}
		for j := 0; j < 20; j++ {
			d = append(d, randVertex())
		}
		kgraph.AddVertex(d)
	}
}

func graphBenchRun(kv kvi.KVInterface, f func(kgraph gdbi.GraphInterface)) {
	db := kvgraph.NewKVGraph(kv)
	graph := "bench-graph"
	db.AddGraph(graph)
	kgraph, err := db.Graph(graph)
	if err != nil {
		return
	}
	logBenchmark(func() {
		f(kgraph)
	})
}

func badgerBench(graphPath string) {
	kv, err := kvgraph.NewKVInterface("badger", graphPath, &kvi.Options{BulkLoad: true})
	if err != nil {
		return
	}
	graphBenchRun(kv, randomVertexInsert)
	kv.Close()
	//os.RemoveAll(graphPath)
}

func levelBench(graphPath string) {
	kv, err := kvgraph.NewKVInterface("level", graphPath, &kvi.Options{})
	if err != nil {
		return
	}
	graphBenchRun(kv, randomVertexInsert)
	kv.Close()
	os.RemoveAll(graphPath)
}

func rocksBench(graphPath string) {
	kv, err := kvgraph.NewKVInterface("rocks", graphPath, &kvi.Options{})
	if err != nil {
		log.Printf("Rocks Failed to init")
		return
	}
	graphBenchRun(kv, randomVertexInsert)
	kv.Close()
	os.RemoveAll(graphPath)
}

func main() {
	log.Printf("Starting Benchmark")
	badgerBench("test.db")
	//levelBench("test.db")
	//rocksBench("test.db")
}
