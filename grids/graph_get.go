package grids

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
)

type idEntry struct {
	lookup gdbi.ElementLookup
	loc    *benchtop.RowLoc
	label  string
	fields []string
	data   map[string]any
	idx    int
}

type lookupPriv struct {
	loc    *benchtop.RowLoc
	fields []string
	data   map[string]any
	uid    uint64
}

const resolveBatchSize = 20000

func (ggraph *Graph) resolveBatch(ctx context.Context, batch []gdbi.ElementLookup, out chan gdbi.ElementLookup, isEdge bool) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()

	var withLoc []idEntry
	var missingIdx []int
	var keys []string
	var uidMissingIdx []int
	var uidMissingVals []uint64

	for i, id := range batch {
		var entry *benchtop.RowLoc
		var fields []string
		var label string
		if id.Priv != nil {
			if loc, ok := id.Priv.(*benchtop.RowLoc); ok {
				entry = loc
			} else if loc, ok := id.Priv.(benchtop.RowLoc); ok {
				entry = &loc
			} else if priv, ok := id.Priv.(*lookupPriv); ok && priv != nil {
				entry = priv.loc
				fields = priv.fields
				if priv.data != nil {
					withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, data: priv.data, idx: i})
					continue
				}
				if entry == nil && priv.uid != 0 && !isEdge {
					uidMissingIdx = append(uidMissingIdx, i)
					uidMissingVals = append(uidMissingVals, priv.uid)
					continue
				}
			} else if priv, ok := id.Priv.(lookupPriv); ok {
				entry = priv.loc
				fields = priv.fields
				if priv.data != nil {
					withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, data: priv.data, idx: i})
					continue
				}
				if entry == nil && priv.uid != 0 && !isEdge {
					uidMissingIdx = append(uidMissingIdx, i)
					uidMissingVals = append(uidMissingVals, priv.uid)
					continue
				}
			}
		}
		if id.Vertex != nil {
			label = id.Vertex.Get().Label
		} else if id.Edge != nil {
			label = id.Edge.Get().Label
		}
		if entry != nil {
			if id.Edge != nil && id.Edge.Get() != nil && id.Edge.Get().Data != nil {
				withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, data: id.Edge.Get().Data, idx: i})
				continue
			}
			if label == "" {
				if t, err := ggraph.driver.GetTableByID(entry.TableId); err == nil {
					label = t.Label
				}
			}
			withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, idx: i})
		} else {
			missingIdx = append(missingIdx, i)
			keys = append(keys, id.ID)
		}
	}

	if len(uidMissingVals) > 0 {
		locsByUID, err := ggraph.driver.GetVertexLocByUIDBatch(ctx, uidMissingVals)
		if err != nil {
			log.Errorf("resolveBatch: GetVertexLocByUIDBatch error: %v", err)
		}
		for j, idx := range uidMissingIdx {
			id := batch[idx]
			info := locsByUID[uidMissingVals[j]]
			if info != nil {
				var fields []string
				if id.Priv != nil {
					if priv, ok := id.Priv.(*lookupPriv); ok && priv != nil {
						fields = priv.fields
					} else if priv, ok := id.Priv.(lookupPriv); ok {
						fields = priv.fields
					}
				}
				withLoc = append(withLoc, idEntry{lookup: id, loc: info.Loc, label: info.Label, fields: fields, data: info.Data, idx: idx})
			} else {
				// Fallback to string-ID lookup
				missingIdx = append(missingIdx, idx)
				keys = append(keys, id.ID)
			}
		}
	}

	if len(keys) > 0 {
		locs, err := ggraph.driver.GetLocBatch(ctx, keys)
		if !isEdge {
			locs, err = ggraph.driver.GetVertexLocBatch(ctx, keys)
		}
		if err != nil {
			log.Errorf("resolveBatch: GetLocBatch error: %v", err)
		}
		for _, idx := range missingIdx {
			id := batch[idx]
			info := locs[id.ID]
			if info != nil {
				var fields []string
				if id.Priv != nil {
					if priv, ok := id.Priv.(*lookupPriv); ok && priv != nil {
						fields = priv.fields
					} else if priv, ok := id.Priv.(lookupPriv); ok {
						fields = priv.fields
					}
				}
				withLoc = append(withLoc, idEntry{lookup: id, loc: info.Loc, label: info.Label, fields: fields, data: info.Data, idx: idx})
			}
		}
	}

	if len(withLoc) > 0 {
		if isEdge {
			ggraph.processEdgeBatch(withLoc, out)
		} else {
			ggraph.processVertexBatch(withLoc, out)
		}
	}
	if len(batch) >= 1000 {
		unresolved := len(batch) - len(withLoc)
		log.Debugf("resolveBatch done isEdge=%v input=%d withLoc=%d unresolved=%d elapsed=%s", isEdge, len(batch), len(withLoc), unresolved, time.Since(start).Round(time.Millisecond))
	}
}

func projectRowMap(row map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return row
	}
	out := map[string]any{}
	for _, f := range fields {
		if v, ok := row[f]; ok {
			out[f] = v
		}
	}
	// Always preserve structural fields if present, as they might be needed for downstream processors
	for _, f := range []string{"_id", "_label", "_from", "_to"} {
		if v, ok := row[f]; ok {
			out[f] = v
		}
	}
	return out
}

func (ggraph *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		if !load {
			for id := range ids {
				if ctx.Err() != nil {
					return
				}
				if id.IsSignal() {
					select {
					case <-ctx.Done():
						return
					case out <- id:
					}
					continue
				}
				id.Vertex = &gdbi.Vertex{ID: id.ID, Label: labelFromElementID(id.ID)}
				select {
				case <-ctx.Done():
					return
				case out <- id:
				}
			}
			return
		}
		var batch []gdbi.ElementLookup
		for id := range ids {
			if ctx.Err() != nil {
				return
			}
			if id.IsSignal() {
				if len(batch) > 0 {
					ggraph.resolveBatch(ctx, batch, out, false)
					batch = nil
				}
				select {
				case <-ctx.Done():
					return
				case out <- id:
				}
				continue
			}
			batch = append(batch, id)
			if len(batch) >= resolveBatchSize {
				ggraph.resolveBatch(ctx, batch, out, false)
				batch = nil
			}
		}
		if len(batch) > 0 {
			ggraph.resolveBatch(ctx, batch, out, false)
		}
	}()
	return out
}

func (ggraph *Graph) processVertexBatch(batch []idEntry, out chan gdbi.ElementLookup) {
	byTable := make(map[uint16][]idEntry)
	for _, entry := range batch {
		byTable[entry.loc.TableId] = append(byTable[entry.loc.TableId], entry)
	}

	maxIdx := -1
	for _, entry := range batch {
		if entry.idx > maxIdx {
			maxIdx = entry.idx
		}
	}
	ordered := make([]*gdbi.ElementLookup, maxIdx+1)

	for tid, entries := range byTable {
		locs := make([]*benchtop.RowLoc, len(entries))
		for i, entry := range entries {
			locs[i] = entry.loc
		}

		table, err := ggraph.driver.GetTableByID(tid)
		var results []map[string]any
		var errors []error
		if err != nil || table == nil {
			log.Errorf("processVertexBatch: table ID %d not found", tid)
			errors = make([]error, len(entries))
			for i := range errors {
				errors[i] = fmt.Errorf("table not found")
			}
			continue
		} else {
			results, errors = table.GetRows(locs)
		}

		for i, entry := range entries {
			id := entry.lookup
			if id.Vertex == nil {
				id.Vertex = &gdbi.Vertex{ID: id.ID, Label: entry.label}
			}
			var res map[string]any
			if entry.data != nil {
				res = entry.data
			} else if errors != nil && errors[i] == nil {
				res = results[i]
			} else {
				continue
			}
			id.Vertex.Get().Data = projectRowMap(res, entry.fields)
			id.Vertex.Get().Loaded = true
			ordered[entry.idx] = &id
		}
	}

	for _, v := range ordered {
		if v != nil {
			out <- *v
		}
	}
}

func (ggraph *Graph) processEdgeBatch(batch []idEntry, out chan gdbi.ElementLookup) {
	byTable := make(map[uint16][]idEntry)
	for _, entry := range batch {
		byTable[entry.loc.TableId] = append(byTable[entry.loc.TableId], entry)
	}

	maxIdx := -1
	for _, entry := range batch {
		if entry.idx > maxIdx {
			maxIdx = entry.idx
		}
	}
	ordered := make([]*gdbi.ElementLookup, maxIdx+1)

	for tid, entries := range byTable {
		locs := make([]*benchtop.RowLoc, len(entries))
		for i, entry := range entries {
			locs[i] = entry.loc
		}
		table, err := ggraph.driver.GetTableByID(tid)
		var results []map[string]any
		var errors []error
		if err != nil || table == nil {
			log.Errorf("processEdgeBatch: table ID %d not found", tid)
			errors = make([]error, len(entries))
			for i := range errors {
				errors[i] = fmt.Errorf("table not found")
			}
			continue
		} else {
			results, errors = table.GetRows(locs)
		}
		for i, entry := range entries {
			id := entry.lookup
			if id.Edge == nil {
				id.Edge = &gdbi.Edge{ID: id.ID, Label: entry.label}
			}
			var res map[string]any
			if entry.data != nil {
				res = entry.data
			} else if errors != nil && errors[i] == nil {
				res = results[i]
			} else {
				continue
			}
			id.Edge.Get().Data = projectRowMap(res, entry.fields)
			if from, ok := res["_from"].(string); ok {
				id.Edge.Get().From = from
			} else if id.Edge.Get().From == "" {
				log.Errorf("processEdgeBatch: edge %s missing _from", id.ID)
				continue
			}
			if to, ok := res["_to"].(string); ok {
				id.Edge.Get().To = to
			} else if id.Edge.Get().To == "" {
				log.Errorf("processEdgeBatch: edge %s missing _to", id.ID)
				continue
			}
			if label, ok := res["_label"].(string); ok {
				id.Edge.Get().Label = label
			}
			id.Edge.Get().Loaded = true
			ordered[entry.idx] = &id
		}
	}

	for _, v := range ordered {
		if v != nil {
			out <- *v
		}
	}
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *Graph) GetVertex(id string, loadProp bool) *gdbi.Vertex {
	uid, _ := ggraph.driver.GetID(id)
	vkey := key.VertexKey(uid)
	val, closer, err := ggraph.driver.Pkv.Get(vkey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		log.Errorf("GetVertex Pkv.Get error: %v", err)
		return nil
	}
	defer closer.Close()

	label, loc := benchtop.DecodeVertexValue(val)
	v := &gdbi.Vertex{
		ID:    id,
		Label: label,
	}

	if loadProp {
		tableStore, terr := ggraph.driver.GetOrLoadTable("v_" + v.Label)
		if terr != nil {
			log.Errorf("GetVertex: table load error: %v", terr)
			return nil
		}
		v.Data, err = tableStore.GetRow(loc)
		if err != nil {
			log.Errorf("GetVertex: table.GetRow( error: %v", err)
			return nil
		}

		v.Data = projectRowMap(v.Data, nil)

		v.Loaded = true
	} else {
		v.Data = map[string]any{}
	}
	return v
}

// GetEdge loads an edge given an id. It returns nil if not found
func (ggraph *Graph) GetEdge(id string, loadProp bool) *gdbi.Edge {
	uid, _ := ggraph.driver.GetID(id)
	ekeyPrefix := key.EdgeKeyPrefix(uid)
	var e *gdbi.Edge
	var byteVal []byte
	err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			euid, suid, duid, label := key.EdgeKeyParse(it.Key())
			eid, _ := ggraph.driver.TranslateID(euid)
			src, _ := ggraph.driver.TranslateID(suid)
			dst, _ := ggraph.driver.TranslateID(duid)

			byteVal, _ = it.Value()
			e = &gdbi.Edge{
				ID:    eid,
				From:  src,
				To:    dst,
				Label: label,
			}
			if loadProp {
				lbl, loc, data := benchtop.DecodeEdgeValue(byteVal)
				if data != nil {
					e.Data = projectRowMap(data, nil)
					e.Loaded = true
					return nil
				}

				if loc == nil {
					log.Errorf("GetEdge: integrated key missing RowLoc for %s", e.ID)
					continue
				}

				tableStore, terr := ggraph.driver.GetOrLoadTable("e_" + lbl)
				if terr != nil {
					log.Errorf("GetEdge: table load error: %v", terr)
					continue
				}

				var gerr error
				e.Data, gerr = tableStore.GetRow(loc)
				if gerr != nil {
					log.Errorf("GetEdge: GetRow error: %v", gerr)
					continue
				}
				e.Data = projectRowMap(e.Data, nil)

				e.Loaded = true

			} else {
				e.Data = map[string]any{}
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return e
}

// GetVertexList produces a channel of all edges in the graph
func (ggraph *Graph) GetVertexList(ctx context.Context, loadProp bool) <-chan *gdbi.Vertex {
	o := make(chan *gdbi.Vertex, 100)
	go func() {
		defer close(o)
		ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			vPrefix := key.VertexListPrefix()
			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				byteVal, err := it.Value()
				if err != nil {
					log.Errorf("GetVertexList it.Value() error: %s", err)
				}
				label, loc := benchtop.DecodeVertexValue(byteVal)
				vid, _ := ggraph.driver.TranslateID(key.VertexKeyParse(it.Key()))
				v := &gdbi.Vertex{
					ID:    vid,
					Label: label,
				}
				if loadProp {
					if loc == nil {
						log.Errorf("GetVertexList: integrated key missing RowLoc for %s", v.ID)
						continue
					}

					tableStore, terr := ggraph.driver.GetOrLoadTable("v_" + v.Label)
					if terr != nil {
						log.Errorf("GetVertexList: table load error: %v", terr)
						continue
					}
					v.Data, err = tableStore.GetRow(loc)
					if err != nil {
						log.Errorf("GetVertexList: table.GetRow error: %s", err)
						continue
					}
					v.Data = projectRowMap(v.Data, nil)

					v.Loaded = true

				} else {
					v.Data = map[string]any{}
				}
				o <- v
			}
			return nil
		})
	}()
	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (ggraph *Graph) ListVertexLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.driver.GetLabels(false, true) {
		labels = append(labels, i)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.driver.GetLabels(true, true) {
		labels = append(labels, i)
	}
	return labels, nil
}
