package merge

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type MergeGraphDB struct {
	Graphs map[string]GraphConfig
}

// AddGraph implements [gdbi.GraphDB].
func (m *MergeGraphDB) AddGraph(string) error {
	panic("unimplemented")
}

// BuildSchema implements [gdbi.GraphDB].
func (m *MergeGraphDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
	panic("unimplemented")
}

// Close implements [gdbi.GraphDB].
func (m *MergeGraphDB) Close() error {
	panic("unimplemented")
}

// DeleteGraph implements [gdbi.GraphDB].
func (m *MergeGraphDB) DeleteGraph(string) error {
	panic("unimplemented")
}

// Graph implements [gdbi.GraphDB].
func (m *MergeGraphDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	panic("unimplemented")
}

// ListGraphs implements [gdbi.GraphDB].
func (m *MergeGraphDB) ListGraphs() []string {
	panic("unimplemented")
}

func NewGraphDB(config *Config) (gdbi.GraphDB, error) {
	return &MergeGraphDB{
		Graphs: config.Graphs,
	}, nil
}
