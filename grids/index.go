package grids

import (
	"context"
	"fmt"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
)

// AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	tableLabel := key.VertexTablePrefix + label
	id, err := ggraph.driver.TableDr.LookupTableID(tableLabel)
	if err != nil {
		// Attempt to create the table if it doesn't exist
		if _, err := ggraph.driver.New(tableLabel, nil); err != nil {
			return fmt.Errorf("AddVertexIndex: failed to create table %s: %v", tableLabel, err)
		}
		// Lookup again
		id, err = ggraph.driver.TableDr.LookupTableID(tableLabel)
		if err != nil {
			return fmt.Errorf("AddVertexIndex: table lookup failed after creation %s: %v", tableLabel, err)
		}
	}
	return ggraph.driver.AddField(id, field)
}

// DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(label, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	id, err := ggraph.driver.TableDr.LookupTableID(key.VertexTablePrefix + label)
	if err != nil {
		return err
	}
	return ggraph.driver.RemoveField(id, field)
}

// GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		for _, f := range ggraph.driver.ListFields() {
			label := f.Label
			if len(label) > 2 && label[:2] == key.VertexTablePrefix {
				label = label[2:]
			}
			out <- &gripql.IndexID{Graph: ggraph.graphID, Label: label, Field: f.Field}
		}
	}()
	return out
}

// VertexLabelScan produces a channel of all vertex ids in a graph
// that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	if len(label) < 2 || label[:2] != key.VertexTablePrefix {
		label = key.VertexTablePrefix + label
	}
	log.WithFields(log.Fields{"label": label}).Info("Running VertexLabelScan")
	return ggraph.driver.GetIDsForLabel(label)
}

func (ggraph *Graph) DeleteAnyRow(id string, label string, edgeFlag bool) error {
	var prefix string = "v_"
	if edgeFlag {
		prefix = "e_"
	}

	loc, err := ggraph.driver.LocCache.Get(context.Background(), id)
	if err != nil {
		return err
	}

	tableLabel := prefix + label
	var bulkErr *multierror.Error
	table, err := ggraph.driver.GetOrLoadTable(tableLabel)
	hasTable := (err == nil && table != nil)
	if hasTable {
		// Verify lineage
		if table.TableId != loc.TableId {
			log.Warningf("table mismatch during delete of %s: index says %s (ID %d) but row loc says TableID %d; using loc TableID", id, tableLabel, table.TableId, loc.TableId)
			// Use GetTableInfo instead of LabelLookup
			if info, err := ggraph.driver.TableDr.GetTableInfo(loc.TableId); err == nil {
				name := info.Name
				// Ensure it is of the right type (v_ or e_)
				if len(name) > 2 && name[:2] == prefix {
					if realTable, err := ggraph.driver.GetOrLoadTable(name); err == nil {
						table = realTable
					}
				}
			}
		}
		for field := range table.Fields {
			if err := ggraph.driver.DeleteRowField(loc.TableId, field, id); err != nil {
				log.Errorf("Failed to delete index for field '%s' in table ID %d for row '%s': %v", field, loc.TableId, id, err)
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
	}

	ggraph.driver.PebbleLock.Lock()
	defer ggraph.driver.PebbleLock.Unlock()

	bId := []byte(id)
	err = ggraph.driver.Pkv.Delete(benchtop.NewPosKey(loc.TableId, bId), nil)
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if hasTable {
		err = table.DeleteRow(loc, bId)
		if err != nil {
			if err == pebble.ErrNotFound {
				log.Debugf("Pebble not Found: %s", err)
			} else {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
	} else {
		log.Warningf("table %s not found in driver.Tables during delete of row %s; skipping data storage deletion but continuing with index cleanup", tableLabel, id)
	}

	ggraph.driver.LocCache.Invalidate(id)
	ggraph.driver.TableDr.InvalidateLoc(loc.TableId, id)
	return bulkErr.ErrorOrNil()
}
