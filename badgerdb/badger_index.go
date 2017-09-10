package badgerdb

import (
	"context"
)

func (self *BadgerGDB) VertexLabelScan(ctx context.Context, label string) chan string {
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

func (self *BadgerGDB) EdgeLabelScan(ctx context.Context, label string) chan string {
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
