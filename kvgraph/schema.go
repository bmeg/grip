package kvgraph

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
)

// GetSchema gets schema of the graph
func (kgraph *KVGraph) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
	return nil, fmt.Errorf("KV Schema not implemented")
}
