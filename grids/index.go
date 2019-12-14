package grids

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
	log "github.com/sirupsen/logrus"
)

func (kgraph *GDB) setupGraphIndex(graph string) error {
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

func (kgraph *GDB) deleteGraphIndex(graph string) {
	fields := kgraph.idx.ListFields()
	for _, f := range fields {
		t := strings.Split(f, ".")
		if t[0] == graph {
			kgraph.idx.RemoveField(f)
		}
	}
}

func normalizePath(path string) string {
	path = jsonpath.GetJSONPath(path)
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "data.")
	return path
}

func vertexIdxStruct(v *gripql.Vertex) map[string]interface{} {
	k := map[string]interface{}{
		"v": map[string]interface{}{
			"label": v.Label,
			v.Label: protoutil.AsMap(v.Data),
		},
	}
	return k
}

func edgeIdxStruct(e *gripql.Edge) map[string]interface{} {
	k := map[string]interface{}{
		"e": map[string]interface{}{
			"label": e.Label,
			e.Label: protoutil.AsMap(e.Data),
		},
	}
	return k
}

//AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Adding vertex index")
	field = normalizePath(field)
	//TODO kick off background process to reindex existing data
	return ggraph.kdb.idx.AddField(fmt.Sprintf("%s.v.%s", ggraph.graphID, field))
}

//DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return ggraph.kdb.idx.RemoveField(fmt.Sprintf("%s.v.%s", ggraph.graphID, field))
}

//GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		fields := ggraph.kdb.idx.ListFields()
		for _, f := range fields {
			t := strings.Split(f, ".")
			if len(t) > 2 {
				out <- &gripql.IndexID{Graph: ggraph.graphID, Field: t[2]}
			}
		}
	}()
	return out
}

//VertexLabelScan produces a channel of all vertex ids in a graph
//that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//log.Printf("Searching %s %s", fmt.Sprintf("%s.label", ggraph.graph), label)
		for i := range ggraph.kdb.idx.GetTermMatch(ctx, fmt.Sprintf("%s.v.label", ggraph.graphID), label, 0) {
			//log.Printf("Found: %s", i)
			out <- i
		}
	}()
	return out
}

// VertexIndexScan produces a channel of all vertex ids where the indexed field matches the query string
func (ggraph *Graph) VertexIndexScan(ctx context.Context, query *gripql.IndexQuery) <-chan string {
	log.WithFields(log.Fields{"query": query}).Debug("Running VertexIndexScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//TODO: Implement prefix matching
		for i := range ggraph.kdb.idx.GetTermMatch(ctx, fmt.Sprintf("%s.v.%s", ggraph.graphID, query.Field), query.Value, 0) {
			out <- i
		}
	}()
	return out
}
