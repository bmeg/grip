package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"runtime/pprof"

	"github.com/bmeg/grip/engine"
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
var vertexLabelValues = []string{
	"Person",
	"Place",
	"Thing",
}
var edgeLabelValues = []string{
	"knows",
	"likes",
	"hears",
}

var fieldNames = []string{
	"firstName",
	"lastName",
	"city",
	"state",
	"ssn",
	"dob",
	"favoriteColor",
}

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}

func randVertexLabel() string {
	return vertexLabelValues[rand.Intn(len(vertexLabelValues))]
}

func randEdgeLabel() string {
	return edgeLabelValues[rand.Intn(len(edgeLabelValues))]
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
		Label: randVertexLabel(),
		Data:  protoutil.AsStruct(randData()),
	}
	return &g
}

func randOneToMany(outCount int) (*gripql.Vertex, []*gripql.Edge, []*gripql.Vertex) {
	a := randVertex()
	oV := make([]*gripql.Vertex, outCount)
	oE := make([]*gripql.Edge, outCount)
	for i := 0; i < outCount; i++ {
		oV[i] = randVertex()
		oE[i] = &gripql.Edge{From: a.Gid, To: oV[i].Gid, Label: randEdgeLabel()}
	}
	return a, oE, oV
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

func randomOneToManyInsert(kgraph gdbi.GraphInterface) {
	for i := 0; i < 50000; i++ {
		v, oe, ov := randOneToMany(3)
		kgraph.AddVertex([]*gripql.Vertex{v})
		kgraph.AddVertex(ov)
		kgraph.AddEdge(oe)
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

func graphBenchRunQuery(kv kvi.KVInterface, build, query func(kgraph gdbi.GraphInterface)) {
	db := kvgraph.NewKVGraph(kv)
	graph := "bench-graph"
	db.AddGraph(graph)
	kgraph, err := db.Graph(graph)
	if err != nil {
		return
	}
	build(kgraph)

	f, err := os.Create("query.cpu_profile")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	for i := 0; i < 3; i++ {
		logBenchmark(func() {
			query(kgraph)
		})
	}
	pprof.StopCPUProfile()
}

func manyToOneQuery(kgraph gdbi.GraphInterface) {
	query := gripql.V().Where(gripql.Eq("_label", "Person")).Out("knows").Count()
	comp := kgraph.Compiler()
	pipe, err := comp.Compile(query.Statements)
	if err != nil {
		log.Printf("%s", err)
	}

	o := engine.Run(context.Background(), pipe, "tmp-work")
	for i := range o {
		log.Printf("%s", i)
	}
}

func badgerBench(graphPath string) {
	kv, err := kvgraph.NewKVInterface("badger", graphPath, &kvi.Options{BulkLoad: true})
	if err != nil {
		return
	}
	//graphBenchRun(kv, randomVertexInsert)
	graphBenchRunQuery(kv, randomOneToManyInsert, manyToOneQuery)
	kv.Close()
	os.RemoveAll(graphPath)
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
