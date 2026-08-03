package graphbench

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bmeg/grip/gripql"

	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
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

func RandID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}

func RandVertexLabel() string {
	return vertexLabelValues[rand.Intn(len(vertexLabelValues))]
}

func RandEdgeLabel() string {
	return edgeLabelValues[rand.Intn(len(edgeLabelValues))]
}

func RandData() map[string]any {
	o := map[string]any{}
	for _, i := range fieldNames {
		o[i] = RandID()
	}
	return o
}

func RandomVertex() *gripql.Vertex {
	randData := RandData()
	g := gripql.Vertex{
		Id:    RandID(),
		Label: RandVertexLabel(),
	}
	g.SetDataMap(randData)
	return &g
}

func RandOneToMany(outCount int) (*gripql.Vertex, []*gripql.Edge, []*gripql.Vertex) {
	a := RandomVertex()
	oV := make([]*gripql.Vertex, outCount)
	oE := make([]*gripql.Edge, outCount)
	for i := 0; i < outCount; i++ {
		oV[i] = RandomVertex()
		oE[i] = &gripql.Edge{From: a.Id, To: oV[i].Id, Label: RandEdgeLabel()}
	}
	return a, oE, oV
}

func LogBenchmark(f func()) {
	start := time.Now()
	f()
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Time: %s", elapsed)
}

func RandomVertexInsert(kgraph gripql.Client, graph string) {
	for i := 0; i < 10000; i++ {
		d := []*gripql.Vertex{}
		for j := 0; j < 20; j++ {
			d = append(d, RandomVertex())
		}
		kgraph.AddVertexArray(graph, d)
	}
}

func RandomOneToManyInsert(kgraph gripql.Client, graph string) {
	for i := 0; i < 50000; i++ {
		v, oe, ov := RandOneToMany(3)
		kgraph.AddVertex(graph, v)
		kgraph.AddVertexArray(graph, ov)
		kgraph.AddEdgeArray(graph, oe)
	}
}

func ManyToOneQuery(kgraph gripql.Client, graph string) {
	query := gripql.V().HasLabel("Person").Out("knows").Count()
	res, err := kgraph.Traversal(context.Background(), &gripql.GraphQuery{Graph: graph, Query: query.Statements})
	if err != nil {
		log.Printf("%s", err)
	}
	for i := range res {
		log.Printf("%+v", i)
	}
}

func randVertex() *gripql.Vertex {
	d := randData()
	v := gripql.Vertex{Id: randID(), Label: randVertexLabel()}
	v.SetDataMap(d)
	return &v
}

func randVectorElement() float64 {
	return float64(rand.Intn(100)) / 100.0
}

func makeLargeVector(size int) []float64 {
	v := make([]float64, size)
	for i := 0; i < size; i++ {
		v[i] = randVectorElement()
	}
	return v
}

func randLargeVertex(vectorSize int) *gripql.Vertex {
	dataMap := map[string]any{}
	dataMap["embedding"] = makeLargeVector(vectorSize)
	v := gripql.Vertex{
		Id:    fmt.Sprintf("largevec-%d", rand.Intn(100)),
		Label: "LargeVecNode",
	}
	v.SetDataMap(dataMap)
	return &v
}

func randVertexLabel() string { return vertexLabelValues[rand.Intn(len(vertexLabelValues))] }
func randEdgeLabel() string   { return edgeLabelValues[rand.Intn(len(edgeLabelValues))] }

func randData() map[string]any {
	o := map[string]any{}
	for _, i := range fieldNames {
		o[i] = randID()
	}
	return o
}

// ---------------------------------------------------------------------------
// One‑to‑many helpers – used only for embedded path
func randomOneToManyInsert(g gripql.Client, graph string) {
	a, oe, ov := RandOneToMany(3)

	g.AddVertex(graph, a)

	g.AddVertexArray(graph, ov)
	g.AddEdgeArray(graph, oe)
}

func randomOneToMany(outCount int) (*gripql.Vertex, []*gripql.Edge, []*gripql.Vertex) {
	a := randVertex()
	oV := make([]*gripql.Vertex, outCount)
	oE := make([]*gripql.Edge, outCount)
	for i := 0; i < outCount; i++ {
		oV[i] = randVertex()
		oE[i] = &gripql.Edge{From: a.Id, To: oV[i].Id, Label: randEdgeLabel()}
	}
	return a, oE, oV
}

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}
