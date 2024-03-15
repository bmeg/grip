package index

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/index/kvindex"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"
)

type KeyMarshaller interface {
}

type GraphKVIndex struct {
	kv  kvi.KVInterface
	idx *kvindex.KVIndex
}

// RebuildIndex implements gdbi.Index.
func (kgdb *GraphKVIndex) RebuildIndex(label string) {
	panic("unimplemented")
}

// WrapGraph implements gdbi.Index.
func (kgdb *GraphKVIndex) WrapGraph(gi gdbi.GraphInterface) gdbi.GraphInterface {
	panic("unimplemented")
}

func NewGKVIndex(kv kvi.KVInterface) (gdbi.Index, error) {
	return &GraphKVIndex{
		kv: kv, idx: kvindex.NewIndex(kv),
	}, nil
}

// AddVertexIndex add index to vertices
func (kgdb *GraphKVIndex) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	field = normalizePath(field)
	//TODO kick off background process to reindex existing data
	return kgdb.idx.AddField(fmt.Sprintf("v.%s.%s", label, field))
}

// DeleteVertexIndex delete index from vertices
func (kgdb *GraphKVIndex) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return kgdb.idx.RemoveField(fmt.Sprintf("v.%s.%s", label, field))
}

// GetVertexIndexList lists out all the vertex indices for a graph
func (kgdb *GraphKVIndex) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		fields := kgdb.idx.ListFields()
		for _, f := range fields {
			t := strings.Split(f, ".")
			if len(t) > 3 {
				out <- &gripql.IndexID{Graph: kgdb.graph, Label: t[2], Field: t[3]}
			}
		}
	}()
	return out
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (kgdb *GraphKVIndex) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//log.Printf("Searching %s %s", fmt.Sprintf("%s.label", kgdb.graph), label)
		for i := range kgdb.kvg.idx.GetTermMatch(ctx, fmt.Sprintf("%s.v.label", kgdb.graph), label, 0) {
			//log.Printf("Found: %s", i)
			out <- i
		}
	}()
	return out
}

func (kgraph *GraphKVIndex) setupGraphIndex(graph string) error {
	err := kgraph.idx.AddField(fmt.Sprintf("%s.v.label", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on vertex label")
	}
	err = kgraph.idx.AddField(fmt.Sprintf("%s.e.label", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on edge label")
	}
	return nil
}

func (kgraph *GraphKVIndex) deleteGraphIndex(graph string) {
	fields := kgraph.idx.ListFields()
	for _, f := range fields {
		t := strings.Split(f, ".")
		if t[0] == graph {
			kgraph.idx.RemoveField(f)
		}
	}
}

func normalizePath(path string) string {
	//path = travelerpath.GetJSONPath(path)
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "data.")
	return path
}

func vertexIdxStruct(v *gripql.Vertex) map[string]interface{} {
	k := map[string]interface{}{
		"v": map[string]interface{}{
			"label": v.Label,
			v.Label: v.Data.AsMap(),
		},
	}
	return k
}

func edgeIdxStruct(e *gripql.Edge) map[string]interface{} {
	k := map[string]interface{}{
		"e": map[string]interface{}{
			"label": e.Label,
			e.Label: e.Data.AsMap(),
		},
	}
	return k
}

func insertVertex(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, vertex *gripql.Vertex) error {
	doc := map[string]interface{}{graph: vertexIdxStruct(vertex)}
	if err := idx.AddDocTx(tx, vertex.Gid, doc); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, edge *gripql.Edge) error {
	eid := edge.Gid
	err := idx.AddDocTx(tx, eid, map[string]interface{}{graph: edgeIdxStruct(edge)})
	if err != nil {
		return err
	}
	return nil
}
