package grids

import (
	"context"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	return ggraph.bsonkv.AddField(VTABLE_PREFIX+label, field)
}

// DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	return ggraph.bsonkv.RemoveField(VTABLE_PREFIX+label, field, nil, nil)
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

// Vertex Filter Scan produces a channel of all vertex ids in a graph that match the field - value filter
func (ggraph *Graph) VertexHasConditionScan(ctx context.Context, field string, value string) chan string {
	log.WithFields(log.Fields{"field": field, "value": value}).Info("Running VertexFilterScan")
	return ggraph.bsonkv.RowIdsByFieldValue(field, value)
}

// Vertex Filter Scan produces a channel of all vertex ids in a graph that match the field - value filter
func (ggraph *Graph) VertexFilterLabelScan(ctx context.Context, label string, field string, value string) (chan string, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "value": value}).Info("Running VertexFilterLabelScan")
	if label[:2] != VTABLE_PREFIX {
		label = VTABLE_PREFIX + label
	}
	return ggraph.bsonkv.RowIdsByLabelFieldValue(label, field, value)
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Info("Running VertexLabelScan")
	if label[:2] != VTABLE_PREFIX {
		label = VTABLE_PREFIX + label
	}
	return ggraph.bsonkv.GetIDsForLabel(label)
}
