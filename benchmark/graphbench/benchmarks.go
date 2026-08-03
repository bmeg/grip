package graphbench

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bmeg/grip/gripql"
)

var Benchmarks = []Benchmark{
	clientInsertVertices,
	clientQueryKnowsCount,
	clientLargeVectorInsert,
}

func RunBenchmarks(ctx context.Context, c *gripql.Client, graph string, logBenchmark LogResult, backend string) {
	// Simple benchmark: insert many vertices and run a query
	// Insert 10k random vertices using bulk mutation

	for _, b := range Benchmarks {
		start := time.Now()
		result := b(ctx, c, graph)
		elapsed := time.Since(start)
		for _, r := range result {
			r.Backend = backend
			if r.Time == 0 {
				r.Time = elapsed.Seconds()
			}
			logBenchmark(r)
		}
	}
}

func clientInsertVertices(ctx context.Context, c *gripql.Client, graph string) []Result {
	result := Result{Name: "clientInsertVertices", Scale: 10000, Meta: map[string]any{}}
	for i := 0; i < 10000; i++ {
		v := randVertex()
		err := c.AddVertex(graph, v)
		if err != nil {
			log.Fatalf("Insert error: %v", err)
		}
	}
	return []Result{result}
}

func clientQueryKnowsCount(ctx context.Context, c *gripql.Client, graph string) []Result {
	result := Result{Name: "clientQueryKnowsCount", Scale: 10000, Meta: map[string]any{}}
	query := gripql.V().HasLabel("Person").Out("knows").Count()
	res, err := c.Traversal(ctx, &gripql.GraphQuery{Graph: graph, Query: query.Statements})
	if err != nil {
		log.Fatalf("Query error: %v", err)
		result.Error = err
	}
	fmt.Printf("Knows count result: %+v\n", res)
	return []Result{result}
}

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
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

func clientLargeVectorInsert(ctx context.Context, c *gripql.Client, graph string) []Result {
	insertResult := Result{
		Name:  "clientLargeVectorInsert",
		Scale: 100,
		Meta:  map[string]any{"vectorSize": 10000},
	}

	// Insert 100 vertices each with a 10K-element vector stored in its data map
	insertStart := time.Now()
	for i := 0; i < 100; i++ {
		v := randLargeVertex(10000)
		v.Id = fmt.Sprintf("vecnode-%d", i)
		v.Label = "LargeVecNode"
		if err := c.AddVertex(graph, v); err != nil {
			log.Fatalf("LargeVector insert error: %v", err)
		}
	}
	insertResult.Time = time.Since(insertStart).Seconds()

	// Read back all 100 vertices and inspect vector sizes
	readResults := Result{
		Name:  "clientLargeVectorRead",
		Scale: 100,
		Meta:  map[string]any{"vectorSize": 10000},
	}
	readStart := time.Now()
	traversal := gripql.V().HasLabel("LargeVecNode")
	res, err := c.Traversal(ctx, &gripql.GraphQuery{
		Graph: graph,
		Query: traversal.Statements,
	})
	if err != nil {
		log.Fatalf("LargeVector read error: %v", err)
	}
	readResults.Time = time.Since(readStart).Seconds()
	fmt.Printf("Large vector query result count: %d\n", len(res))

	return []Result{insertResult, readResults}
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
