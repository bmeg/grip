package kvgraph

import (
	"context"
)

// Field
// Key: 'f' | field_id
// Val: graph, type

// Term
// Key: 't' | field_id | term
// Val: term_id

// TermDoc
// Key: 'd' | field_id | term_id | doc_id

// FieldKeyParse : returns field int
func FieldKeyParse(k []byte) (int) {

}

func TermKeyParse(k []byte) ()


const (
	FieldString = iota
	FieldInt    
	FieldFloat
)

func (kgdb *KVInterfaceGDB) AddIndex(graph, field string) {

}

func (kgdb *KVInterfaceGDB) DeleteIndex(graph, field string) {

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
