package graphbench

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bmeg/grip/gripql"
)

var Benchmarks = []Benchmark{
	Benchmark_InsertVerticesSingle,
	Benchmark_InsertVerticesBulk,
	Benchmark_QueryKnowsCount,
	Benchmark_LargeVectorInsert,
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

func Benchmark_InsertVerticesSingle(ctx context.Context, c *gripql.Client, graph string) []Result {
	result := Result{Name: "InsertVerticesSingle", Scale: 10000, Meta: map[string]any{}}
	for i := 0; i < 10000; i++ {
		v := randVertex()
		err := c.AddVertex(graph, v)
		if err != nil {
			log.Fatalf("Insert error: %v", err)
		}
	}
	return []Result{result}
}

func Benchmark_InsertVerticesBulk(ctx context.Context, c *gripql.Client, graph string) []Result {
	result := Result{Name: "InsertVerticesBulk", Scale: 10000, Meta: map[string]any{}}
	upload := []*gripql.Vertex{}
	for i := 0; i < 10000; i++ {
		v := randVertex()
		upload = append(upload, v)
	}
	err := c.AddVertexArray(graph, upload)
	if err != nil {
		log.Fatalf("Insert error: %v", err)
	}
	return []Result{result}
}

func Benchmark_QueryKnowsCount(ctx context.Context, c *gripql.Client, graph string) []Result {
	result := Result{Name: "QueryKnowsCount", Scale: 10000, Meta: map[string]any{}}
	query := gripql.V().HasLabel("Person").Out("knows").Count()
	res, err := c.Traversal(ctx, &gripql.GraphQuery{Graph: graph, Query: query.Statements})
	if err != nil {
		log.Fatalf("Query error: %v", err)
		result.Error = err
	}
	fmt.Printf("Knows count result: %+v\n", res)
	return []Result{result}
}

func Benchmark_LargeVectorInsert(ctx context.Context, c *gripql.Client, graph string) []Result {
	insertResult := Result{
		Name:  "LargeVectorInsert",
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
		Name:  "LargeVectorRead",
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
