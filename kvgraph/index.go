package kvgraph

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
)

func (kgraph *KVGraph) setupGraphIndex(graph string) error {
	err := kgraph.idx.AddField(fmt.Sprintf("%s.vlabel", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on vertex label")
	}
	err = kgraph.idx.AddField(fmt.Sprintf("%s.elabel", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on edge label")
	}
	return nil
}

func (kgraph *KVGraph) deleteGraphIndex(graph string) {
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
		"v": protoutil.AsMap(v.Data),
	}
	k["vlabel"] = v.Label
	return k
}

func edgeIdxStruct(e *gripql.Edge) map[string]interface{} {
	k := map[string]interface{}{
		"elabel": e.Label,
	}
	return k
}

//AddVertexIndex add index to vertices
func (kgdb *KVInterfaceGDB) AddVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Adding vertex index")
	field = normalizePath(field)
	//TODO kick off background process to reindex existing data
	return kgdb.kvg.idx.AddField(fmt.Sprintf("%s.v.%s", kgdb.graph, field))
}

//DeleteVertexIndex delete index from vertices
func (kgdb *KVInterfaceGDB) DeleteVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return kgdb.kvg.idx.RemoveField(fmt.Sprintf("%s.v.%s", kgdb.graph, field))
}

//GetVertexIndexList lists out all the vertex indices for a graph
func (kgdb *KVInterfaceGDB) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		fields := kgdb.kvg.idx.ListFields()
		for _, f := range fields {
			t := strings.Split(f, ".")
			if len(t) > 2 {
				out <- &gripql.IndexID{Graph: kgdb.graph, Field: t[2]}
			}
		}
	}()
	return out
}

// ListVertexLabels returns a list of vertex types in the graph
func (kgdb *KVInterfaceGDB) ListVertexLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.vlabel", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (kgdb *KVInterfaceGDB) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.elabel", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

//VertexLabelScan produces a channel of all vertex ids in a graph
//that match a given label
func (kgdb *KVInterfaceGDB) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//log.Printf("Searching %s %s", fmt.Sprintf("%s.label", kgdb.graph), label)
		for i := range kgdb.kvg.idx.GetTermMatch(ctx, fmt.Sprintf("%s.vlabel", kgdb.graph), label, 0) {
			//log.Printf("Found: %s", i)
			out <- i
		}
	}()
	return out
}

// VertexIndexScan produces a channel of all vertex ids where the indexed field matches the query string
func (kgdb *KVInterfaceGDB) VertexIndexScan(ctx context.Context, query *gripql.SearchQuery) <-chan string {
	log.WithFields(log.Fields{"query": query}).Debug("Running VertexIndexScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range kgdb.kvg.idx.GetTermPrefixMatch(ctx, fmt.Sprintf("%s.v.%s", kgdb.graph, query.Fields), query.Term, 0) {
			out <- i
		}
	}()
	return out
}
