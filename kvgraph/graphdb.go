package kvgraph

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvi"
)

// AddGraph creates a new graph named `graph`
func (kgraph *KVGraph) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	kgraph.ts.Touch(graph)
	err = kgraph.setupGraphIndex(graph)
	if err != nil {
		return err
	}
	return kgraph.kv.Set(GraphKey(graph), []byte{})
}

// DeleteGraph deletes `graph`
func (kgraph *KVGraph) DeleteGraph(graph string) error {
	kgraph.ts.Touch(graph)

	eprefix := EdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(eprefix)

	vprefix := VertexListPrefix(graph)
	kgraph.kv.DeletePrefix(vprefix)

	sprefix := SrcEdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(sprefix)

	dprefix := DstEdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(dprefix)

	graphKey := GraphKey(graph)
	kgraph.kv.Delete(graphKey)

	kgraph.deleteGraphIndex(graph)

	return nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *KVGraph) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range kgraph.ListGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &KVInterfaceGDB{kvg: kgraph, graph: graph}, nil
}

// Close the connection
func (kgraph *KVGraph) Close() error {
	return kgraph.kv.Close()
}

// ListGraphs lists the graphs managed by this driver
func (kgraph *KVGraph) ListGraphs() []string {
	out := make([]string, 0, 100)
	gPrefix := GraphPrefix()
	kgraph.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(gPrefix); it.Valid() && bytes.HasPrefix(it.Key(), gPrefix); it.Next() {
			out = append(out, GraphKeyParse(it.Key()))
		}
		return nil
	})
	return out
}

// GetSchema returns the schema of a specific graph in the database
func (kgraph *KVGraph) GetSchema(ctx context.Context, graph string, sampleN uint32) (*gripql.GraphSchema, error) {
	return nil, fmt.Errorf("not implemented")
}
