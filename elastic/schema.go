package elastic

import (
	"context"

	"github.com/bmeg/grip/gdbi/schema"
	"github.com/bmeg/grip/gripql"
)

// BuildSchema returns the schema of a specific graph in the database
func (es *GraphDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {

	gr, err := es.Graph(graph)
	if err != nil {
		return nil, err
	}

	return schema.SchemaScan(ctx, graph, gr, sampleN, random)
}
