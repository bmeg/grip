package grids

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/driver"
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

type projectedRowsGetter interface {
	GetRowsProjected(locs []*benchtop.RowLoc, fields []string) ([]map[string]any, []error)
}

type rawRowsGetter interface {
	GetRowsRawPayload(locs []*benchtop.RowLoc) ([]string, []error)
}

const resolveBatchSize = 20000

func lookupLocFromMeta(meta gdbi.LookupMeta) *benchtop.RowLoc {
	switch loc := meta.Opaque.(type) {
	case *benchtop.RowLoc:
		return loc
	case benchtop.RowLoc:
		l := loc
		return &l
	default:
		return nil
	}
}

func (ggraph *Graph) resolveBatch(ctx context.Context, batch []gdbi.ElementLookup, out chan gdbi.ElementLookup, isEdge bool) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()

	// Resolve numeric IDs in one shot when callers provide only UID-based lookup
	// metadata for traversal throughput.
	idsToTranslate := make([]uint64, 0, len(batch)*3)
	for i := range batch {
		id := batch[i]
		meta, ok := id.GetLookupMeta()
		if !ok {
			continue
		}
		if id.ID == "" && meta.UID != 0 {
			idsToTranslate = append(idsToTranslate, meta.UID)
		}
		if isEdge && id.Edge != nil {
			e := id.Edge
			if e.ID == "" && meta.EUID != 0 {
				idsToTranslate = append(idsToTranslate, meta.EUID)
			}
			if e.From == "" && meta.SUID != 0 {
				idsToTranslate = append(idsToTranslate, meta.SUID)
			}
			if e.To == "" && meta.DUID != 0 {
				idsToTranslate = append(idsToTranslate, meta.DUID)
			}
		}
	}
	if len(idsToTranslate) > 0 {
		rids, err := ggraph.driver.TranslateIDs(idsToTranslate)
		if err != nil {
			log.Errorf("resolveBatch: TranslateIDs error: %v", err)
		} else {
			for i := range batch {
				id := batch[i]
				meta, ok := id.GetLookupMeta()
				if !ok {
					continue
				}
				if id.ID == "" && meta.UID != 0 {
					if rid, ok := rids[meta.UID]; ok {
						batch[i].ID = rid
					}
				}
				if isEdge && id.Edge != nil {
					e := id.Edge
					if e.ID == "" && meta.EUID != 0 {
						if rid, ok := rids[meta.EUID]; ok {
							e.ID = rid
						}
					}
					if e.From == "" && meta.SUID != 0 {
						if rid, ok := rids[meta.SUID]; ok {
							e.From = rid
						}
					}
					if e.To == "" && meta.DUID != 0 {
						if rid, ok := rids[meta.DUID]; ok {
							e.To = rid
						}
					}
				}
			}
		}
	}

	var withLoc []idEntry
	var missingIdx []int
	var keys []string
	var uidMissingIdx []int
	var uidMissingVals []uint64

	for i, id := range batch {
		var entry *benchtop.RowLoc
		var fields []string
		var data map[string]any
		var uid uint64
		var label string
		if meta, ok := id.GetLookupMeta(); ok {
			entry = lookupLocFromMeta(meta)
			fields = meta.Fields
			data = meta.Data
			uid = meta.UID
		}
		if data != nil {
			withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, data: data, idx: i})
			continue
		}
		if entry == nil && uid != 0 && !isEdge {
			uidMissingIdx = append(uidMissingIdx, i)
			uidMissingVals = append(uidMissingVals, uid)
			continue
		}
		if id.Vertex != nil {
			label = id.Vertex.GetLabel()
		} else if id.Edge != nil {
			label = id.Edge.GetLabel()
		}
		if entry != nil {
			if id.Edge != nil && id.Edge.Mode() != gdbi.RowModeRaw {
				if payload := id.Edge.GetPayload(); payload != nil {
					withLoc = append(withLoc, idEntry{lookup: id, loc: entry, label: label, fields: fields, data: payload, idx: i})
					continue
				}
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
				if meta, ok := id.GetLookupMeta(); ok {
					fields = meta.Fields
				}
				withLoc = append(withLoc, idEntry{lookup: id, loc: info.Loc, label: info.Label, fields: fields, data: info.Data, idx: idx})
			} else {
				// Fallback to string-ID lookup
				if id.ID != "" {
					missingIdx = append(missingIdx, idx)
					keys = append(keys, id.ID)
				}
			}
		}
	}

	if len(keys) > 0 {
		var (
			locs map[string]*driver.IDInfo
			err  error
		)
		if isEdge {
			locs, err = ggraph.driver.GetLocBatch(ctx, keys)
		} else {
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
				if meta, ok := id.GetLookupMeta(); ok {
					fields = meta.Fields
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
	if row == nil {
		return nil
	}
	if len(fields) == 0 {
		delete(row, "_id")
		delete(row, "_label")
		delete(row, "_from")
		delete(row, "_to")
		return row
	}
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		if f == "_id" || f == "_label" || f == "_from" || f == "_to" {
			continue
		}
		if v, ok := row[f]; ok {
			out[f] = v
		}
	}
	return out
}

func normalizeProjectedFetchFields(fields []string) ([]string, bool) {
	if len(fields) == 0 {
		return nil, false
	}
	out := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		if field == "" || field == "_id" || field == "_label" || field == "_from" || field == "_to" {
			continue
		}
		if strings.Contains(field, ".") || strings.Contains(field, "[") {
			return nil, false
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	if len(out) == 0 {
		return nil, false
	}
	sort.Strings(out)
	return out, true
}

func normalizeProjectedEdgeFetchFields(fields []string) ([]string, bool) {
	out, ok := normalizeProjectedFetchFields(fields)
	if !ok {
		return nil, false
	}
	seen := map[string]struct{}{}
	for _, f := range out {
		seen[f] = struct{}{}
	}
	if _, ok := seen["_from"]; !ok {
		out = append(out, "_from")
	}
	if _, ok := seen["_to"]; !ok {
		out = append(out, "_to")
	}
	if _, ok := seen["_label"]; !ok {
		out = append(out, "_label")
	}
	sort.Strings(out)
	return out, true
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
		table, err := ggraph.driver.GetTableByID(tid)
		var results []map[string]any
		var rawResults []string
		var errors []error
		if err != nil || table == nil {
			log.Errorf("processVertexBatch: table ID %d not found", tid)
			errors = make([]error, len(entries))
			for i := range errors {
				errors[i] = fmt.Errorf("table not found")
			}
			continue
		} else {
			results = make([]map[string]any, len(entries))
			rawResults = make([]string, len(entries))
			errors = make([]error, len(entries))

			projectedGetter, canProject := table.TableStore.(projectedRowsGetter)
			rawGetter, canRaw := table.TableStore.(rawRowsGetter)
			fullFetchIdx := make([]int, 0, len(entries))
			rawFetchIdx := make([]int, 0, len(entries))
			projectedFetchIdx := map[string][]int{}
			projectedFetchFields := map[string][]string{}

			for i, entry := range entries {
				if entry.data != nil {
					continue
				}
				if len(entry.fields) == 0 && canRaw {
					rawFetchIdx = append(rawFetchIdx, i)
					continue
				}
				if !canProject || len(entry.fields) == 0 {
					fullFetchIdx = append(fullFetchIdx, i)
					continue
				}
				fields, ok := normalizeProjectedFetchFields(entry.fields)
				if !ok {
					fullFetchIdx = append(fullFetchIdx, i)
					continue
				}
				key := strings.Join(fields, "\x1f")
				projectedFetchIdx[key] = append(projectedFetchIdx[key], i)
				projectedFetchFields[key] = fields
			}

			fetchFull := func(idxs []int) {
				if len(idxs) == 0 {
					return
				}
				locs := make([]*benchtop.RowLoc, len(idxs))
				for i, idx := range idxs {
					locs[i] = entries[idx].loc
				}
				rows, errs := table.GetRows(locs)
				for i, idx := range idxs {
					if i < len(rows) {
						results[idx] = rows[i]
					}
					if i < len(errs) {
						errors[idx] = errs[i]
					}
				}
			}

			fetchFull(fullFetchIdx)
			if canRaw && len(rawFetchIdx) > 0 {
				locs := make([]*benchtop.RowLoc, len(rawFetchIdx))
				for i, idx := range rawFetchIdx {
					locs[i] = entries[idx].loc
				}
				rows, errs := rawGetter.GetRowsRawPayload(locs)
				fallback := len(rows) != len(rawFetchIdx) || len(errs) != len(rawFetchIdx)
				if !fallback {
					for i, idx := range rawFetchIdx {
						if errs[i] != nil || rows[i] == "" {
							fallback = true
							fullFetchIdx = append(fullFetchIdx, idx)
						} else {
							rawResults[idx] = rows[i]
						}
					}
				}
				if fallback {
					need := make([]int, 0, len(rawFetchIdx))
					seen := map[int]struct{}{}
					for _, idx := range rawFetchIdx {
						if _, ok := seen[idx]; ok {
							continue
						}
						if rawResults[idx] != "" {
							continue
						}
						seen[idx] = struct{}{}
						need = append(need, idx)
					}
					fetchFull(need)
				}
			}
			if canProject {
				for key, idxs := range projectedFetchIdx {
					locs := make([]*benchtop.RowLoc, len(idxs))
					for i, idx := range idxs {
						locs[i] = entries[idx].loc
					}
					rows, errs := projectedGetter.GetRowsProjected(locs, projectedFetchFields[key])
					fallback := len(rows) != len(idxs)
					if !fallback {
						for i := range idxs {
							if i < len(errs) && errs[i] != nil {
								fallback = true
								break
							}
						}
					}
					if fallback {
						fetchFull(idxs)
						continue
					}
					for i, idx := range idxs {
						results[idx] = rows[i]
					}
				}
			}
		}

		for i, entry := range entries {
			id := entry.lookup
			if id.Vertex == nil {
				id.Vertex = &gdbi.Vertex{ID: id.ID, Label: entry.label}
			}
			var res map[string]any
			if entry.data != nil {
				res = entry.data
			} else if rawResults[i] != "" && len(entry.fields) == 0 {
				de := id.Vertex
				de.Data = nil
				de.RawJSON = rawResults[i]
				de.Loaded = true
				ordered[entry.idx] = &id
				continue
			} else if errors[i] == nil {
				res = results[i]
			} else {
				continue
			}
			de := id.Vertex
			de.Data = projectRowMap(res, entry.fields)
			de.RawJSON = ""
			de.Loaded = true
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
			results = make([]map[string]any, len(entries))
			errors = make([]error, len(entries))

			projectedGetter, canProject := table.TableStore.(projectedRowsGetter)
			fullFetchIdx := make([]int, 0, len(entries))
			projectedFetchIdx := map[string][]int{}
			projectedFetchFields := map[string][]string{}

			for i, entry := range entries {
				if entry.data != nil {
					continue
				}
				if !canProject || len(entry.fields) == 0 {
					fullFetchIdx = append(fullFetchIdx, i)
					continue
				}
				fields, ok := normalizeProjectedEdgeFetchFields(entry.fields)
				if !ok {
					fullFetchIdx = append(fullFetchIdx, i)
					continue
				}
				key := strings.Join(fields, "\x1f")
				projectedFetchIdx[key] = append(projectedFetchIdx[key], i)
				projectedFetchFields[key] = fields
			}

			fetchFull := func(idxs []int) {
				if len(idxs) == 0 {
					return
				}
				locs := make([]*benchtop.RowLoc, len(idxs))
				for i, idx := range idxs {
					locs[i] = entries[idx].loc
				}
				rows, errs := table.GetRows(locs)
				for i, idx := range idxs {
					if i < len(rows) {
						results[idx] = rows[i]
					}
					if i < len(errs) {
						errors[idx] = errs[i]
					}
				}
			}

			fetchFull(fullFetchIdx)
			if canProject {
				for key, idxs := range projectedFetchIdx {
					locs := make([]*benchtop.RowLoc, len(idxs))
					for i, idx := range idxs {
						locs[i] = entries[idx].loc
					}
					rows, errs := projectedGetter.GetRowsProjected(locs, projectedFetchFields[key])
					fallback := len(rows) != len(idxs)
					if !fallback {
						for i := range idxs {
							if i < len(errs) && errs[i] != nil {
								fallback = true
								break
							}
						}
					}
					if fallback {
						fetchFull(idxs)
						continue
					}
					for i, idx := range idxs {
						results[idx] = rows[i]
					}
				}
			}
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
			de := id.Edge
			if from, ok := res["_from"].(string); ok {
				de.From = from
			}
			if to, ok := res["_to"].(string); ok {
				de.To = to
			}
			if label, ok := res["_label"].(string); ok {
				de.Label = label
			}
			de.Data = projectRowMap(res, entry.fields)
			if de.From == "" {
				log.Errorf("processEdgeBatch: edge %s missing _from", id.ID)
				continue
			}
			if de.To == "" {
				log.Errorf("processEdgeBatch: edge %s missing _to", id.ID)
				continue
			}
			de.Loaded = true
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
				if from, ok := e.Data["_from"].(string); ok {
					e.From = from
				}
				if to, ok := e.Data["_to"].(string); ok {
					e.To = to
				}
				if label, ok := e.Data["_label"].(string); ok {
					e.Label = label
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
