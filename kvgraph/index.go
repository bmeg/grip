package kvgraph

import (
	"context"
)

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
