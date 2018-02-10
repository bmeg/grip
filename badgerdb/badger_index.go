package badgerdb

import (
	"context"
	"github.com/bmeg/arachne/aql"
)


func (bgdb *BadgerGDB) AddVertexIndex(label string, field string) error {

	return nil
}

func (bgdb *BadgerGDB) AddEdgeIndex(label string, field string) error {

	return nil
}

func (bgdb *BadgerGDB) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {

	return nil
}

func (bgdb *BadgerGDB) GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {

	return nil
}

func (bgdb *BadgerGDB) DeleteVertexIndex(label string, field string) error {

	return nil
}

func (bgdb *BadgerGDB) DeleteEdgeIndex(label string, field string) error {

	return nil
}


// VertexLabelScan produces a channel of all edge ids where the edge label matches `label`
func (bgdb *BadgerGDB) VertexLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range bgdb.GetVertexList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
		}
	}()
	return out
}

// EdgeLabelScan produces a channel of all edge ids where the edge label matches `label`
func (bgdb *BadgerGDB) EdgeLabelScan(ctx context.Context, label string) chan string {
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		for i := range bgdb.GetEdgeList(ctx, true) {
			if i.Label == label {
				out <- i.Gid
			}
		}
	}()
	return out
}
