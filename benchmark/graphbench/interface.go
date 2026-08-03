package graphbench

import (
	"context"

	"github.com/bmeg/grip/gripql"
)

type Result struct {
	Name    string
	Backend string
	Time    float64
	Scale   int
	Meta    map[string]any
	Error   error
}

type Benchmark func(ctx context.Context, c *gripql.Client, graph string) []Result

type LogResult func(result Result)
