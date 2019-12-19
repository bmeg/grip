package grids

import (
	"bytes"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
)

// AddGraph creates a new graph named `graph`
func (kgraph *GDB) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	kgraph.ts.Touch(graph)
	err = kgraph.setupGraphIndex(graph)
	if err != nil {
		return err
	}
	gkey := kgraph.keyMap.GetGraphKey(graph)
	return kgraph.graphkv.Set(GraphKey(gkey), []byte{})
}

// DeleteGraph deletes `graph`
func (kgraph *GDB) DeleteGraph(graph string) error {
	kgraph.ts.Touch(graph)

	gkey := kgraph.keyMap.GetGraphKey(graph)

	var anyError error

	eprefix := EdgeListPrefix(gkey)
	if err := kgraph.graphkv.DeletePrefix(eprefix); err != nil {
		anyError = err
	}

	vprefix := VertexListPrefix(gkey)
	if err := kgraph.graphkv.DeletePrefix(vprefix); err != nil {
		anyError = err
	}

	sprefix := SrcEdgeListPrefix(gkey)
	if err := kgraph.graphkv.DeletePrefix(sprefix); err != nil {
		anyError = err
	}

	dprefix := DstEdgeListPrefix(gkey)
	if err := kgraph.graphkv.DeletePrefix(dprefix); err != nil {
		anyError = err
	}

	graphKey := GraphKey(gkey)
	if err := kgraph.graphkv.Delete(graphKey); err != nil {
		anyError = err
	}

	if err := kgraph.deleteGraphIndex(graph); err != nil {
		anyError = err
	}

	return anyError
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *GDB) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range kgraph.ListGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	gkey := kgraph.keyMap.GetGraphKey(graph)
	return &Graph{kdb: kgraph, graphID: graph, graphKey: gkey}, nil
}

// ListGraphs lists the graphs managed by this driver
func (kgraph *GDB) ListGraphs() []string {
	out := []string{}
	gPrefix := GraphPrefix()
	kgraph.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(gPrefix); it.Valid() && bytes.HasPrefix(it.Key(), gPrefix); it.Next() {
			g := kgraph.keyMap.GetGraphID(GraphKeyParse(it.Key()))
			out = append(out, g)
		}
		return nil
	})
	return out
}
