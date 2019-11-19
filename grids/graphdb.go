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
	gkey := kgraph.keyMap.GetGraphKey( graph )
	return kgraph.graphkv.Set(GraphKey(gkey), []byte{})
}

// DeleteGraph deletes `graph`
func (kgraph *GDB) DeleteGraph(graph string) error {
	kgraph.ts.Touch(graph)

	gkey := kgraph.keyMap.GetGraphKey( graph )

	eprefix := EdgeListPrefix(gkey)
	kgraph.graphkv.DeletePrefix(eprefix)

	vprefix := VertexListPrefix(gkey)
	kgraph.graphkv.DeletePrefix(vprefix)

	sprefix := SrcEdgeListPrefix(gkey)
	kgraph.graphkv.DeletePrefix(sprefix)

	dprefix := DstEdgeListPrefix(gkey)
	kgraph.graphkv.DeletePrefix(dprefix)

	graphKey := GraphKey(gkey)
	kgraph.graphkv.Delete(graphKey)

	kgraph.deleteGraphIndex(graph)

	return nil
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
			g := kgraph.keyMap.GetGraphID( GraphKeyParse(it.Key()) )
			out = append(out, g)
		}
		return nil
	})
	return out
}
