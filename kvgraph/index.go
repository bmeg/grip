package kvgraph

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	"log"
	"strings"
	//"github.com/bmeg/arachne/kvindex"
)

func (kgraph *KVGraph) setupGraphIndex(graph string) {
	kgraph.idx.AddField(fmt.Sprintf("%s.label", graph))
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

func vertexIdxStruct(v *aql.Vertex) map[string]interface{} {
	//vertexField := fmt.Sprintf("v.%s", v.Label)
	k := map[string]interface{}{
		"label": v.Label,
		"v":     map[string]interface{}{v.Label: protoutil.AsMap(v.Data)},
	}
	//log.Printf("Vertex: %s", k)
	return k
}

//AddVertexIndex add index to vertices
func (kgdb *KVInterfaceGDB) AddVertexIndex(label string, field string) error {
	log.Printf("Adding Index: %s:%s", label, field)
	return kgdb.kvg.idx.AddField(fmt.Sprintf("%s.v.%s.%s", kgdb.graph, label, field))
}

func (kgdb *KVInterfaceGDB) GetVertexIndexList() chan aql.IndexID {
	out := make(chan aql.IndexID)
	go func() {
		defer close(out)
		fields := kgdb.kvg.idx.ListFields()
		for _, f := range fields {
			t := strings.Split(f, ".")
			if len(t) > 3 {
				a := aql.IndexID{Graph: kgdb.graph, Label: t[2], Field: t[3]}
				out <- a
			}
		}
	}()
	return out
}

//GetVertexTermCount get count of every term across vertices
func (kgdb *KVInterfaceGDB) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	log.Printf("Running GetVertexTermCount")
	out := make(chan aql.IndexTermCount, 100)
	go func() {
		defer close(out)
		for tcount := range kgdb.kvg.idx.FieldTermCounts(fmt.Sprintf("%s.v.%s.%s", kgdb.graph, label, field)) {
			s := string(tcount.Value)
			t := protoutil.WrapValue(s)
			a := aql.IndexTermCount{Term: t, Count: int32(tcount.Count)}
			out <- a
		}
	}()
	return out
}

//GetEdgeTermCount get count of every term across edges
func (kgdb *KVInterfaceGDB) GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {

	return nil
}

//DeleteVertexIndex delete index from vertices
func (kgdb *KVInterfaceGDB) DeleteVertexIndex(label string, field string) error {

	return nil
}

//DeleteEdgeIndex delete index from edges
func (kgdb *KVInterfaceGDB) DeleteEdgeIndex(label string, field string) error {

	return nil
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (kgdb *KVInterfaceGDB) VertexLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//log.Printf("Searching %s %s", fmt.Sprintf("%s.label", kgdb.graph), label)
		for i := range kgdb.kvg.idx.GetTermMatch(fmt.Sprintf("%s.label", kgdb.graph), label) {
			//log.Printf("Found: %s", i)
			out <- i
		}
	}()
	return out
}

// EdgeLabelScan produces a channel of all edge ids in a graph
// that match a given label
func (kgdb *KVInterfaceGDB) EdgeLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range kgdb.GetEdgeList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
		}
	}()
	return out
}
