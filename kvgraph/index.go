package kvgraph

import (
	"context"
)

func (self *KVInterfaceGDB) VertexLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range self.GetVertexList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
		}
	}()
	return out
}

func (self *KVInterfaceGDB) EdgeLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range self.GetEdgeList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
		}
	}()
	return out
}
