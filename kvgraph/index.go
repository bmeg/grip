package kvgraph

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	//"github.com/bmeg/arachne/kvindex"
)

func vertexIdxStruct(graph string, v *aql.Vertex) map[string]interface{} {
	k := map[string]interface{}{
		fmt.Sprintf("%s.label", graph): v.Label,
	}
	return k
}

func edgeIdxStruct(graph string, v *aql.Vertex) map[string]interface{} {
	k := map[string]interface{}{
		fmt.Sprintf("%s.label", graph): v.Label,
	}
	return k
}

func (kgdb *KVInterfaceGDB) indexVertex(v *aql.Vertex) {
	//v := vertexIdxStruct(v)
}

//AddVertexIndex add index to vertices
func (kgdb *KVInterfaceGDB) AddVertexIndex(label string, field string) error {
	return nil
}

//AddEdgeIndex add index to edges
func (kgdb *KVInterfaceGDB) AddEdgeIndex(label string, field string) error {

	return nil
}

//GetVertexTermCount get count of every term across vertices
func (kgdb *KVInterfaceGDB) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {

	return nil
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
		for i := range kgdb.GetVertexList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
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
