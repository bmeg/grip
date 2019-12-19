package grids

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
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

func (kgraph *GDB) deleteGraphIndex(graph string) error {
	var anyError error
	fields := kgraph.idx.ListFields()
	for _, f := range fields {
		t := strings.Split(f, ".")
		if t[0] == graph {
			if err := kgraph.idx.RemoveField(f); err != nil {
				anyError = err
			}
		}
	}
	return anyError
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
func (ggraph *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	field = normalizePath(field)
	//TODO kick off background process to reindex existing data
	return ggraph.kdb.idx.AddField(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field))
}

//DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return ggraph.kdb.idx.RemoveField(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field))
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
			if len(t) > 3 {
				out <- &gripql.IndexID{Graph: ggraph.graphID, Label: t[2], Field: t[3]}
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
