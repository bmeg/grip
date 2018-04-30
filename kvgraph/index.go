package kvgraph

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

func (kgraph *KVGraph) setupGraphIndex(graph string) error {
	return kgraph.idx.AddField(fmt.Sprintf("%s.label", graph))
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
	log.Printf("Adding index: %s.%s", label, field)
	//TODO kick off background process to reindex existing data
	return kgdb.kvg.idx.AddField(fmt.Sprintf("%s.v.%s.%s", kgdb.graph, label, field))
}

//DeleteVertexIndex delete index from vertices
func (kgdb *KVInterfaceGDB) DeleteVertexIndex(label string, field string) error {
	log.Printf("Deleting index: %s.%s", label, field)
	return kgdb.kvg.idx.RemoveField(fmt.Sprintf("%s.v.%s.%s", kgdb.graph, label, field))
}

//GetVertexIndexList lists out all the vertex indices for a graph
func (kgdb *KVInterfaceGDB) GetVertexIndexList() chan aql.IndexID {
	log.Printf("Getting index list")
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

//VertexLabelScan produces a channel of all vertex ids in a graph
//that match a given label
func (kgdb *KVInterfaceGDB) VertexLabelScan(ctx context.Context, label string) chan string {
	log.Printf("Running VertexLabelScan for label: %s", label)
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

//GetVertexTermAggregation get count of every term across vertices
func (kgdb *KVInterfaceGDB) GetVertexTermAggregation(ctx context.Context, name string, label string, field string, size uint32) (*aql.NamedAggregationResult, error) {
	log.Printf("Running GetVertexTermCount: { label: %s, field: %s }", label, field)
	out := &aql.NamedAggregationResult{
		Name:    name,
		Buckets: []*aql.AggregationResult{},
	}

	for tcount := range kgdb.kvg.idx.FieldTermCounts(fmt.Sprintf("%s.v.%s.%s", kgdb.graph, label, field)) {
		s := tcount.String //BUG: This is ignoring number terms
		t := protoutil.WrapValue(s)
		out.Buckets = append(out.Buckets, &aql.AggregationResult{Key: t, Value: float64(tcount.Count)})
	}
	return out, nil
}

//GetVertexHistogramAggregation get binned counts of a term across vertices
func (kgdb *KVInterfaceGDB) GetVertexHistogramAggregation(ctx context.Context, name string, label string, field string, interval uint32) (*aql.NamedAggregationResult, error) {
	return nil, fmt.Errorf("not implemented")
}

//GetVertexPercentileAggregation get percentiles of a term across vertices
func (kgdb *KVInterfaceGDB) GetVertexPercentileAggregation(ctx context.Context, name string, label string, field string, percents []uint32) (*aql.NamedAggregationResult, error) {
	return nil, fmt.Errorf("not implemented")
}
