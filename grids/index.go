package grids

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
)

// AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	return ggraph.jsonkv.AddField(VTABLE_PREFIX+label, field)
}

// DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label, field string) error {
	fmt.Println("HELLO WE HARE HERE")
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	return ggraph.jsonkv.RemoveField(VTABLE_PREFIX+label, field)
}

// GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		for _, f := range ggraph.jsonkv.ListFields() {
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
	return ggraph.jsonkv.GetIDsForLabel(label)
}

func (ggraph *Graph) DeleteAnyRow(id string, label string, edgeFlag bool) error {
	var prefix string = "v_"
	if edgeFlag {
		prefix = "e_"
	}

	loc, err := ggraph.jsonkv.PageCache.Get(context.Background(), id, ggraph.jsonkv.PageLoader)
	if err != nil {
		return err
	}

	tableLabel := prefix + label
	var bulkErr *multierror.Error
	if fields, exists := ggraph.jsonkv.Fields[tableLabel]; exists {
		for field := range fields {
			if err := ggraph.jsonkv.DeleteRowField(tableLabel, field, id); err != nil {
				log.Errorf("Failed to delete index for field '%s' in table '%s' for row '%s': %v", field, tableLabel, id, err)
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
	}

	ggraph.jsonkv.PebbleLock.Lock()
	defer ggraph.jsonkv.PebbleLock.Unlock()

	table, ok := ggraph.jsonkv.Tables[prefix+label]
	if !ok {
		bulkErr = multierror.Append(bulkErr, fmt.Errorf("table %s not found in jsonkv.Tables: %#v", prefix+label, ggraph.jsonkv.Tables))
		return bulkErr.ErrorOrNil()
	}

	err = table.DeleteRow(loc, []byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			log.Debugf("Pebble not Found: %s", err)
			return nil
		}
		bulkErr = multierror.Append(bulkErr, err)
	}
	ggraph.jsonkv.PageCache.Invalidate(id)
	return bulkErr.ErrorOrNil()
}
