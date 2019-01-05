package psql

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
)

// BuildSchema returns the schema of a specific graph in the database
func (db *GraphDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
	return nil, fmt.Errorf("not implemented")
}
