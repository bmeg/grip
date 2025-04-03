package grids

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

func normalizePath(path string) string {
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "data.")
	return path
}

func (ggraph *Graph) BulkAddVertexIndex(idxChan <-chan *gripql.IndexID) error {
	log.Info("BulkAddVertexIndex vertex indices")
	err := ggraph.bsonkv.BulkAddField(idxChan)
	return err
}

// AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	//TODO kick off background process to reindex existing data
	return ggraph.bsonkv.AddFieldIndex(fmt.Sprintf("%s.%s", label, field))
}

// DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return ggraph.bsonkv.RemoveFieldIndex(fmt.Sprintf("%s.%s", label, field))
}

// GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	return ggraph.bsonkv.ListFields(ggraph.graphID)
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	if label[:2] != "v_" {
		label = "v_" + label
	}
	go func() {
		defer close(out)
		log.Infof("Searching %s %s", fmt.Sprintf("%s.label", ggraph.graphID), label)
		for i := range ggraph.bsonkv.GetIDsForLabel(label) {
			out <- i
		}
	}()
	return out
}
