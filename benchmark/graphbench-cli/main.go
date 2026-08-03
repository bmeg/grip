package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/bmeg/grip/benchmark/graphbench"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
)

var (
	serverFlag  = flag.String("server", "localhost:8202", "GRIP server URL (e.g., localhost:8202)")
	graphFlag   = flag.String("graph", "test-graph", "Graph name to use for benchmark")
	backendFlag = flag.String("backend", "client", "Name of the backend being benchmarked (for logging purposes)")
	outputFlag  = flag.String("output", "benchmark-results.json", "Path to write JSON benchmark results")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	runClient(ctx, *serverFlag, *graphFlag, *backendFlag)
}

var results []graphbench.Result = []graphbench.Result{}

// ---------------------------------------------------------------------------
// logBenchmark prints a single result to stdout.
func logBenchmark(result graphbench.Result) {
	fmt.Printf("Benchmark %s took %.2f seconds\n", result.Name, result.Time)
	if result.Error != nil {
		fmt.Printf("Error: %v\n", result.Error)
	}
	results = append(results, result)
}

// writeResults writes all accumulated benchmark results to a JSON file.
func writeResults(results []graphbench.Result, path string) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	fmt.Printf("Wrote %d benchmark result(s) to %s\n", len(results), path)
	return nil
}

// ---------------------------------------------------------------------------
// Client mode – execute queries via GRIP API
func runClient(ctx context.Context, server string, graph string, backend string) {

	client, err := gripql.Connect(rpc.ConfigWithDefaults(server), true)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	grs, err := client.ListGraphs()
	if err != nil {
		log.Fatalf("Failed to list graphs: %v", err)
	}
	found := false
	for _, g := range grs.Graphs {
		if g == graph {
			found = true
			break
		}
	}
	if !found {
		log.Printf("Graph %s not found, creating it...", graph)
		client.AddGraph(graph)
	}
	graphbench.RunBenchmarks(ctx, &client, graph, logBenchmark, backend)

	writeResults(results, *outputFlag)
}
