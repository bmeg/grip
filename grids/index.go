package grids

import (
	"context"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
)

// AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	return ggraph.bsonkv.AddField(VTABLE_PREFIX+label, field)
}

// DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	return ggraph.bsonkv.RemoveField(VTABLE_PREFIX+label, field)
}

// GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		for _, f := range ggraph.bsonkv.ListFields() {
			out <- &gripql.IndexID{Graph: ggraph.graphID, Label: f.Label, Field: f.Field}
		}
	}()
	return out
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	if label[:2] != VTABLE_PREFIX {
		label = VTABLE_PREFIX + label
	}
	log.WithFields(log.Fields{"label": label}).Info("Running VertexLabelScan")
	return ggraph.bsonkv.GetIDsForLabel(label)
}

func (ggraph *Graph) DeleteAnyRow(id string, label string, edgeFlag bool) error {
	ggraph.bsonkv.Lock.Lock()
	defer ggraph.bsonkv.Lock.Unlock()

	var prefix string = "v_"
	if edgeFlag {
		prefix = "e_"
	}

	err := ggraph.bsonkv.Tables[prefix+label].DeleteRow([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound{
			log.Debugln("Pebble not Found: %s", err)
			return nil
		}
		return err
	}
	return nil
}
