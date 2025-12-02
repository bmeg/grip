package grids

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bmeg/benchtop"
	jTable "github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/setcmp"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
)

const (
	VTABLE_PREFIX = "v_"
	ETABLE_PREFIX = "e_"
)

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	return ggraph.ts.Get(ggraph.graphID)
}

func insertVertex(tx *pebblebulk.PebbleBulk, vertex *gdbi.Vertex) error {
	if vertex.ID == "" {
		return fmt.Errorf("inserting null key vertex")
	}
	if err := tx.Set(VertexKey(vertex.ID), []byte(vertex.Label), nil); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func (ggraph *Graph) indexVertex(vertex *gdbi.Vertex, tx *pebblebulk.PebbleBulk) error {
	vertexLabel := VTABLE_PREFIX + vertex.Label
	ggraph.jsonkv.Lock.Lock()
	table, ok := ggraph.jsonkv.Tables[vertexLabel]
	ggraph.jsonkv.Lock.Unlock()
	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", vertexLabel, vertex.Label, ggraph.graphID)
		newTable, err := ggraph.jsonkv.New(vertexLabel, nil)
		if err != nil {
			return fmt.Errorf("indexVertex: %s", err)
		}
		ggraph.jsonkv.Lock.Lock()
		table = newTable.(*jTable.JSONTable)
		ggraph.jsonkv.Tables[vertexLabel] = table
		ggraph.jsonkv.Lock.Unlock()
	}

	rowLoc, err := table.AddRow(
		benchtop.Row{
			Id:        []byte(vertex.ID),
			TableName: vertexLabel,
			Data:      vertex.Data,
		},
	)
	if err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}

	err = ggraph.jsonkv.AddTableEntryInfo(tx, []byte(vertex.ID), rowLoc)
	if err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}

	_, ok = ggraph.jsonkv.LocCache.Set(vertex.ID, rowLoc)
	if !ok {
		ggraph.jsonkv.LocCache.Invalidate(vertex.ID)
		ggraph.jsonkv.LocCache.Set(vertex.ID, rowLoc)
	}

	table, tableExists := ggraph.jsonkv.Tables[vertexLabel]
	if tableExists && len(table.Fields) > 0 {
		for field := range ggraph.jsonkv.Tables[vertexLabel].Fields {
			if val := tpath.PathLookup(vertex.Data, field); val != nil {
				err := tx.Set(benchtop.FieldKey(field, vertexLabel, val, []byte(vertex.ID)), []byte{}, nil)
				if err != nil {
					return err
				}
				Mval, err := sonic.ConfigFastest.Marshal(val)
				if err != nil {
					return err
				}
				err = tx.Set(benchtop.RFieldKey(vertexLabel, field, vertex.ID), Mval, nil)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func insertEdge(tx *pebblebulk.PebbleBulk, edge *gdbi.Edge) error {
	if edge.ID == "" ||
		edge.From == "" ||
		edge.To == "" ||
		edge.Label == "" {
		log.Errorln("insertEdge Err: ", edge)
		return fmt.Errorf("inserting null key edge")
	}
	err := tx.Set(EdgeKey(edge.ID, edge.From, edge.To, edge.Label), nil, nil)
	if err != nil {
		return err
	}
	err = tx.Set(DstEdgeKey(
		edge.ID,
		edge.From,
		edge.To,
		edge.Label,
	), []byte{}, nil)
	if err != nil {
		return err
	}
	err = tx.Set(SrcEdgeKey(
		edge.ID,
		edge.From,
		edge.To,
		edge.Label,
	), []byte{}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (ggraph *Graph) indexEdge(edge *gdbi.Edge, tx *pebblebulk.PebbleBulk) error {
	edgeLabel := ETABLE_PREFIX + edge.Label
	ggraph.jsonkv.Lock.Lock()
	table, ok := ggraph.jsonkv.Tables[edgeLabel]
	ggraph.jsonkv.Lock.Unlock()

	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", edgeLabel, edge.Label, ggraph.graphID)
		newTable, err := ggraph.jsonkv.New(edgeLabel, nil)
		if err != nil {
			return fmt.Errorf("indexEdge: jsonkv.New: %s", err)
		}
		ggraph.jsonkv.Lock.Lock()
		table = newTable.(*jTable.JSONTable)
		ggraph.jsonkv.Tables[edgeLabel] = table
		ggraph.jsonkv.Lock.Unlock()
	}
	rowLoc, err := table.AddRow(benchtop.Row{Id: []byte(edge.ID), TableName: edgeLabel, Data: edge.Data})
	if err != nil {
		return fmt.Errorf("indexEdge: table.AddRow: %s", err)
	}
	err = ggraph.jsonkv.AddTableEntryInfo(tx, []byte(edge.ID), rowLoc)
	if err != nil {
		return fmt.Errorf("indexEdge: jsonkv.AddTableEntryInfo: %s", err)

	}

	_, ok = ggraph.jsonkv.LocCache.Set(edge.ID, rowLoc)
	if !ok {
		ggraph.jsonkv.LocCache.Invalidate(edge.ID)
		ggraph.jsonkv.LocCache.Set(edge.ID, rowLoc)
	}

	table, tableExists := ggraph.jsonkv.Tables[edgeLabel]
	if tableExists && len(table.Fields) > 0 {
		for field := range table.Fields {
			if val := tpath.PathLookup(edge.Data, field); val != nil {
				err := tx.Set(benchtop.FieldKey(field, edgeLabel, val, []byte(edge.ID)), []byte{}, nil)
				if err != nil {
					return err
				}
				eMarsh, err := sonic.ConfigFastest.Marshal(val)
				if err != nil {
					return err
				}
				err = tx.Set(benchtop.RFieldKey(edgeLabel, field, edge.ID), eMarsh, nil)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ggraph *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(ggraph, GridsOptimizer, core.IndexStartOptimize)
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	err := ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, vert); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("AddVertex Error %s", err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})

	err = ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := ggraph.indexVertex(vert, tx); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("IndexVertex Error %s", err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (ggraph *Graph) AddEdge(edges []*gdbi.Edge) error {
	var err error = nil
	err = ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		err = ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for _, edge := range edges {
				err = insertEdge(tx, edge)
				if err != nil {
					log.Errorln("Err insertEdge: ", err)
					return err
				}
			}
			return err
		})
		ggraph.ts.Touch(ggraph.graphID)
		return err
	})
	if err != nil {
		return err
	}
	err = ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := ggraph.indexEdge(edge, tx); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err

}

func (ggraph *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	var errs *multierror.Error
	insertStream := make(chan *gdbi.GraphElement, 100)
	indexStream := make(chan *benchtop.Row, 100)
	errChan := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for elem := range insertStream {
				if elem.Vertex != nil {
					if err := insertVertex(tx, elem.Vertex); err != nil {
						return fmt.Errorf("vertex insert error: %v", err)
					}
				}
				if elem.Edge != nil {
					if err := insertEdge(tx, elem.Edge); err != nil {
						return fmt.Errorf("edge insert error: %v", err)
					}
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("ERR in graph Bulk Add: %s", err)
			return
		}

	}()

	go func() {
		defer wg.Done()
		err := ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			if err := ggraph.jsonkv.BulkLoad(indexStream, tx); err != nil {
				return fmt.Errorf("jsonkv bulk load error: %v", err)
			}
			ggraph.ts.Touch(ggraph.graphID)
			return nil
		})
		errChan <- err
	}()

	go func() {
		defer func() {
			close(insertStream)
			close(indexStream)
		}()
		for elem := range stream {
			insertStream <- elem
			if elem.Vertex != nil {
				indexStream <- &benchtop.Row{
					Id:        []byte(elem.Vertex.ID),
					TableName: VTABLE_PREFIX + elem.Vertex.Label,
					Data:      elem.Vertex.Data,
				}
			}
			if elem.Edge != nil {
				indexStream <- &benchtop.Row{
					Id:        []byte(elem.Edge.ID),
					TableName: ETABLE_PREFIX + elem.Edge.Label,
					Data:      elem.Edge.Data,
				}
			}
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	return errs.ErrorOrNil()
}

func (ggraph *Graph) DelVertex(id string) error {
	vid := VertexKey(id)
	skeyPrefix := SrcEdgePrefix(id)
	dkeyPrefix := DstEdgePrefix(id)

	delKeys := make([][]byte, 0, 1000)
	edgesToDelete := make(map[string]string)

	var bulkErr *multierror.Error
	err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			eid, sid, did, label := SrcEdgeKeyParse(skey)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := EdgeKey(eid, sid, did, label)
			dkey := DstEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)
			edgesToDelete[eid] = label
		}

		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			eid, sid, did, label := DstEdgeKeyParse(dkey)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := EdgeKey(eid, sid, did, label)
			skey := SrcEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)
			edgesToDelete[eid] = label
		}
		return nil
	})

	if err != nil {
		return err
	}

	for eid, label := range edgesToDelete {
		if err := ggraph.DeleteAnyRow(eid, label, true); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}

		if ggraph.tempDeletedEdges != nil {
			ggraph.tempDeletedEdges[eid] = struct{}{}
		}
	}

	loc, err := ggraph.jsonkv.LocCache.Get(context.Background(), id)
	if err != nil {
		return err
	}

	label := ggraph.jsonkv.LabelLookup[loc.TableId]
	if label == "" {
		bulkErr = multierror.Append(bulkErr, fmt.Errorf("Failed to lookup table label %d", loc.TableId))
	}
	if err := ggraph.DeleteAnyRow(id, label, false); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	err = ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.DeletePrefix(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.DeletePrefix(k); err != nil {
				log.Errorf("BulkWrite failed to delete key %s: %v", string(k), err)
				return err
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

func (ggraph *Graph) DelEdge(eid string) error {
	ekeyPrefix := EdgeKeyPrefix(eid)
	var ekey []byte
	err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})
	if err != nil {
		return err
	}

	if ekey == nil {
		log.Debugf("Edge %s not found", eid)
		return nil
	}

	_, sid, did, lbl := EdgeKeyParse(ekey)
	skey := SrcEdgeKey(sid, did, eid, lbl)
	dkey := DstEdgeKey(sid, did, eid, lbl)

	var bulkErr *multierror.Error
	err = ggraph.jsonkv.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(ekey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		if err := tx.Delete(skey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		if err := tx.Delete(dkey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})

	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if err := ggraph.DeleteAnyRow(eid, lbl, true); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *Graph) GetVertex(id string, loadProp bool) *gdbi.Vertex {
	ekeyPrefix := VertexKey(id)
	var byteLabel []byte = nil
	var err error = nil
	err = ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			byteLabel, err = it.Value()
		}
		return nil
	})
	if err != nil || byteLabel == nil {
		return nil
	}

	v := &gdbi.Vertex{
		ID:    id,
		Label: string(byteLabel),
	}
	if loadProp {
		entry, err := ggraph.jsonkv.LocCache.Get(context.Background(), id)
		if err != nil {
			log.Errorf("GetVertex: PageCache.Get( error: %v", err)
			return nil
		}
		v.Data, err = ggraph.jsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
		if err != nil {
			log.Errorf("GetVertex: table.GetRow( error: %v", err)
			return nil
		}
		v.Loaded = true
	} else {
		v.Data = map[string]any{}
	}
	return v
}

type elementData struct {
	label string
	req   gdbi.ElementLookup
	data  []byte
}

type idEntry struct {
	lookup gdbi.ElementLookup
	loc    *benchtop.RowLoc
}

func (ggraph *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		if !load {
			for id := range ids {
				if id.IsSignal() {
					out <- id
				} else {
					id.Vertex = &gdbi.Vertex{ID: id.ID}
					out <- id
				}
			}
			return
		}
		var batch []idEntry
		for id := range ids {
			if id.IsSignal() {
				out <- id
				continue
			}
			entry, err := ggraph.jsonkv.LocCache.Get(ctx, id.ID)
			if err != nil {
				log.Errorf("GetVertexChannel: PageCache.Get error: %v", err)
				continue
			}
			batch = append(batch, idEntry{lookup: id, loc: entry})
			if len(batch) >= 1000 {
				processBatchWithLabelCache(ggraph, batch, out)
				batch = nil
			}
		}
		if len(batch) > 0 {
			processBatchWithLabelCache(ggraph, batch, out)
		}
	}()
	return out
}

type groupKey struct {
	TableId uint16
	Section uint16
}

func processBatchWithLabelCache(ggraph *Graph, batch []idEntry, out chan gdbi.ElementLookup) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	byKey := make(map[groupKey][]idEntry)
	for _, entry := range batch {
		key := groupKey{TableId: entry.loc.TableId, Section: entry.loc.Section}
		byKey[key] = append(byKey[key], entry)
	}
	for key, entries := range byKey {
		wg.Add(1)
		go func(key groupKey, entries []idEntry) {
			sem <- struct{}{}
			defer func() { <-sem; wg.Done() }()
			locs := make([]*benchtop.RowLoc, len(entries))
			for i, entry := range entries {
				locs[i] = entry.loc
			}
			Tlabel := ggraph.jsonkv.LabelLookup[key.TableId]
			results, errors := ggraph.jsonkv.Tables[VTABLE_PREFIX+Tlabel].GetRows(locs, key.Section)
			for i, entry := range entries {
				if errors[i] != nil {
					log.Errorf("GetVertexChannel: GetRows error for ID %s: %v", entry.lookup.ID, errors[i])
					continue
				}
				entry.lookup.Vertex = &gdbi.Vertex{
					Data:   results[i],
					Label:  Tlabel,
					Loaded: true,
					ID:     entries[i].lookup.ID,
				}
				out <- entry.lookup
			}
		}(key, entries)
	}
	wg.Wait()
}

/*
func processBatchWithLabelCache(ggraph *Graph, batch []idEntry, out chan gdbi.ElementLookup) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	bySection := make(map[uint16][]idEntry)
	for _, entry := range batch {
		bySection[entry.loc.Section] = append(bySection[entry.loc.Section], entry)
	}
	for sectionID, entries := range bySection {
		wg.Add(1)
		go func(sectionID uint16, entries []idEntry) {
			sem <- struct{}{}
			defer func() { <-sem; wg.Done() }()
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].loc.Offset < entries[j].loc.Offset
			})
			for _, entry := range entries {
				v := gdbi.Vertex{
					ID:    entry.lookup.ID,
					Label: ggraph.jsonkv.LabelLookup[entry.loc.TableId],
				}
				data, err := ggraph.jsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry.loc)
				if err != nil {
					log.Errorf("GetVertexChannel: GetRow error for ID %s: %v", entry.lookup.ID, err)
					continue
				}
				v.Data = data
				v.Loaded = true
				entry.lookup.Vertex = &v
				out <- entry.lookup
			}
		}(sectionID, entries)
	}
	wg.Wait()
	}*/

type lookup struct {
	req gdbi.ElementLookup
	key string
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	// Todo: implement bulk cache get + bulk get row to try to make this faster
	lookupChan := make(chan lookup, 1000)
	go func() {
		defer close(lookupChan)
		ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					// Use a select statement to send to lookupChan or check for cancellation
					select {
					case lookupChan <- lookup{req: req}:
					case <-ctx.Done():
						return ctx.Err() // Stop if cancelled while trying to send
					}
				} else {
					found := false
					skeyPrefix := SrcEdgePrefix(req.ID)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						_, _, dst, label := SrcEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							lookupChan <- lookup{
								key: dst,
								req: req,
							}
							found = true
						}
					}
					if !found && emitNull {
						lookupChan <- lookup{
							req: req,
							key: "",
						}
					}
				}
			}
			return nil
		})
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for req := range lookupChan {
			if req.req.IsSignal() {
				select {
				case o <- req.req:
				case <-ctx.Done():
					return
				}
			} else {
				if req.key != "" {
					entry, err := ggraph.jsonkv.LocCache.Get(ctx, req.key)
					if err != nil {
						log.Errorf("GetOutChannel: PageCache.Get( error: %v", err)
						continue
					}
					vLabel, ok := ggraph.jsonkv.LabelLookup[entry.TableId]
					if !ok {
						log.Errorf("GetOutChannel: Label not a string %s", vLabel)
						continue
					}
					v := &gdbi.Vertex{ID: req.key, Label: vLabel}
					if load {
						v.Data, err = ggraph.jsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
						if err != nil {
							log.Errorf("GetOutChannel: GetRow on %s: %s error: %v", vLabel, req.key, err)
							continue
						}
						v.Loaded = true
					} else {
						v.Data = map[string]any{}
					}
					req.req.Vertex = v
					o <- req.req
				} else {
					req.req.Vertex = nil
					o <- req.req
				}
			}
		}
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						_, sid, _, label := DstEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							entry, err := ggraph.jsonkv.LocCache.Get(ctx, sid)
							if err != nil {
								log.Errorf("GetInChannel: PageCache.Get( error: %v", err)
								continue
							}

							vLabel, ok := ggraph.jsonkv.LabelLookup[entry.TableId]
							if !ok {
								log.Errorf("GetInChannel Label lookup failed")
								continue
							}

							v := &gdbi.Vertex{ID: sid, Label: vLabel}
							if load {
								v.Data, err = ggraph.jsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
								if err != nil {
									log.Errorf("GetInChannel: GetRow on %s: %s error: %v", vLabel, sid, err)
									continue
								}
								v.Loaded = true
							} else {
								v.Data = map[string]any{}
							}
							req.Vertex = v
							o <- req
							found = true
						}
					}

					if !found && emitNull {
						req.Vertex = nil
						o <- req
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (ggraph *Graph) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					skeyPrefix := SrcEdgePrefix(req.ID)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						eid, src, dst, label := SrcEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							e := gdbi.Edge{
								From:  src,
								To:    dst,
								Label: label,
								ID:    eid,
							}
							if load {
								entry, err := ggraph.jsonkv.LocCache.Get(ctx, e.ID)
								if err != nil {
									log.Errorf("GetOutEdgeChannel: PageCache.Get( error: %v", err)
									continue
								}
								e.Data, err = ggraph.jsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
								if err != nil {
									log.Errorf("GetOutEdgeChannel: GetRow error: %v", err)
									continue
								}
								e.Loaded = true
							} else {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
							found = true
						}
					}

					if !found && emitNull {
						req.Edge = nil
						o <- req
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (ggraph *Graph) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						eid, src, dst, label := DstEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							e := gdbi.Edge{
								ID:    eid,
								From:  src,
								To:    dst,
								Label: label,
							}
							if load {
								entry, err := ggraph.jsonkv.LocCache.Get(ctx, e.ID)
								if err != nil {
									log.Errorf("GetInEdgeChannel: PageCache.Get( error: %v", err)
									continue
								}
								//log.Debugln("IN EDGE LABEL: ", e.Label, "ENTRY: ", entry, "ID: ", e.ID)

								e.Data, err = ggraph.jsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
								if err != nil {
									log.Errorf("GetInEdgeChannel: GetRow error: %v", err)
									continue
								}
								e.Loaded = true
							} else {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
							found = true
						}
					}

					if !found && emitNull {
						req.Edge = nil
						o <- req
					}
				}
			}
			return nil
		})

	}()
	return o
}

// GetEdge loads an edge given an id. It returns nil if not found
func (ggraph *Graph) GetEdge(id string, loadProp bool) *gdbi.Edge {
	ekeyPrefix := EdgeKeyPrefix(id)
	var e *gdbi.Edge
	err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			eid, src, dst, label := EdgeKeyParse(it.Key())
			e = &gdbi.Edge{
				ID:    eid,
				From:  src,
				To:    dst,
				Label: label,
			}
			if loadProp {
				entry, err := ggraph.jsonkv.LocCache.Get(context.Background(), e.ID)
				if err != nil {
					log.Errorf("GetEdge: PageCache.Get( error: %v", err)
					continue
				}

				e.Data, err = ggraph.jsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
				if err != nil {
					log.Errorf("GetEdge: GetRow error: %v", err)
					continue
				}
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
		ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			vPrefix := VertexListPrefix()
			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				byteLabel, err := it.Value()
				if err != nil {
					log.Errorf("GetVertexList it.Value() error: %s", err)
				}
				v := &gdbi.Vertex{
					ID:    VertexKeyParse(it.Key()),
					Label: string(byteLabel),
				}
				if loadProp {
					entry, err := ggraph.jsonkv.LocCache.Get(ctx, v.ID)
					if err != nil {
						log.Errorf("GetVertexList: PageCache.Get on %s error: %s", v.ID, err)
						continue
					}

					v.Data, err = ggraph.jsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
					if err != nil {
						log.Errorf("GetVertexList: table.GetRow error: %s", err)
						continue
					}
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
	for i := range ggraph.jsonkv.GetLabels(false, true) {
		labels = append(labels, i)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.jsonkv.GetLabels(true, true) {
		labels = append(labels, i)
	}
	return labels, nil
}

// New Bulk Delete Function. Testing...
// BulkDel deletes vertices and edges in bulk.
func (ggraph *Graph) BulkDel(data *gdbi.DeleteData) error {
	type keyBatch struct {
		singles [][]byte
		ranges  [][2][]byte
		posKeys [][]byte
	}

	type itemInfo struct {
		id     string
		label  string
		isEdge bool
		tbl    string
	}

	type fieldInfo struct {
		rKey  []byte
		field string
		tbl   string
		id    []byte
	}

	const shardSize = 64
	const bufferSize = 8192
	numCpus := runtime.NumCPU()
	ctx := context.Background()

	var bulkErr *multierror.Error
	addErr := func(err error) {
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}

	// Sharded bitmap for edge deduplication (lock-free for reads)
	type shard struct {
		mu    sync.Mutex
		set   map[string]struct{}
		count uint32 // Atomic counter for seen edges
	}
	shards := make([]*shard, shardSize)
	for i := range shards {
		shards[i] = &shard{set: make(map[string]struct{}, bufferSize/shardSize)}
	}
	hasSeenEdge := func(eid string) bool {
		h := fnv32a(eid) % uint32(shardSize)
		shard := shards[h]
		shard.mu.Lock()
		defer shard.mu.Unlock()
		if _, exists := shard.set[eid]; exists {
			return true
		}
		shard.set[eid] = struct{}{}
		atomic.AddUint32(&shard.count, 1)
		return false
	}
	getSeenCount := func() uint64 {
		var total uint64
		for _, shard := range shards {
			total += uint64(atomic.LoadUint32(&shard.count))
		}
		return total
	}

	// Channels and wait groups
	itemChan := make(chan itemInfo, bufferSize)
	fieldChan := make(chan fieldInfo, bufferSize)
	keyChan := make(chan keyBatch, bufferSize)
	var prodWG, consWG, aggWG, fieldWG sync.WaitGroup

	// Aggregator for keys
	var singles [][]byte
	var ranges [][2][]byte
	var posKeys [][]byte
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		for batch := range keyChan {
			select {
			case <-ctx.Done():
				return
			default:
				singles = append(singles, batch.singles...)
				ranges = append(ranges, batch.ranges...)
				posKeys = append(posKeys, batch.posKeys...)
			}
		}
	}()

	// Aggregator for fields
	var allFields []fieldInfo
	fieldWG.Add(1)
	go func() {
		defer fieldWG.Done()
		for fi := range fieldChan {
			allFields = append(allFields, fi)
		}
	}()

	// Workers for items
	consWG.Add(numCpus)
	for range numCpus {
		go func() {
			defer consWG.Done()
			localBatch := keyBatch{posKeys: make([][]byte, 0, bufferSize)}
			i := 0
			for item := range itemChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if i%100_000 == 0 && i != 0 {
					log.Debugf("[BulkDel worker] processed %d items", i)
				}
				i++

				// Fetch from page cache
				loc, err := ggraph.jsonkv.LocCache.Get(ctx, item.id)
				if err != nil {
					addErr(err)
					continue
				}

				// Mark table for deletion
				table, ok := ggraph.jsonkv.Tables[item.tbl]
				if !ok {
					addErr(fmt.Errorf("table %s not found", item.tbl))
					continue
				}
				if err := table.MarkDeleteTable(loc); err != nil {
					addErr(err)
				}

				// Position key
				localBatch.posKeys = append(localBatch.posKeys, benchtop.NewPosKey(loc.TableId, []byte(item.id)))
				ggraph.jsonkv.LocCache.Invalidate(item.id)

				// Send field infos
				//
				table, tableExists := ggraph.jsonkv.Tables[item.tbl]
				if tableExists && len(table.Fields) > 0 {
					for field := range table.Fields {
						rKey := benchtop.RFieldKey(item.tbl, field, item.id)
						select {
						case fieldChan <- fieldInfo{rKey: rKey, field: field, tbl: item.tbl, id: []byte(item.id)}:
						case <-ctx.Done():
							return
						}
					}
				}

				if len(localBatch.posKeys) >= 500_000 {
					keyChan <- localBatch
					localBatch = keyBatch{posKeys: make([][]byte, 0, bufferSize)}
				}
			}

			if len(localBatch.posKeys) > 0 {
				keyChan <- localBatch
			}
		}()
	}

	// Prepare vertex producers
	slices.Sort(data.Vertices)
	vertexSlices := make([][]string, numCpus)
	for i, vid := range data.Vertices {
		vertexSlices[i%numCpus] = append(vertexSlices[i%numCpus], vid)
	}
	for i := range vertexSlices {
		slices.Sort(vertexSlices[i])
	}

	for _, slice := range vertexSlices {
		if len(slice) == 0 {
			continue
		}
		prodWG.Add(1)
		go func(slice []string) {
			defer prodWG.Done()
			localBatch := keyBatch{singles: make([][]byte, 0, 256), ranges: make([][2][]byte, 0, 256)}

			err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, vid := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					sPrefix := SrcEdgePrefix(vid)
					if err := it.Seek(sPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
						nextPrefix := upperBound(sPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{sPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
							eid, sid, did, lbl := SrcEdgeKeyParse(it.Key())
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									EdgeKey(eid, sid, did, lbl),
									bytes.Clone(it.Key()),
									DstEdgeKey(eid, sid, did, lbl))
								select {
								case itemChan <- itemInfo{id: eid, label: lbl, isEdge: true, tbl: ETABLE_PREFIX + lbl}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
							it.Next()
						}
					}

					dPrefix := DstEdgePrefix(vid)
					if err := it.Seek(dPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
						nextPrefix := upperBound(dPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{dPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
							eid, sid, did, lbl := DstEdgeKeyParse(it.Key())
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									EdgeKey(eid, sid, did, lbl),
									SrcEdgeKey(eid, sid, did, lbl),
									bytes.Clone(it.Key()))
								select {
								case itemChan <- itemInfo{id: eid, label: lbl, isEdge: true, tbl: ETABLE_PREFIX + lbl}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
							it.Next()
						}
					}

					vkey := VertexKey(vid)
					if err := it.Seek(vkey); err != nil {
						return err
					}
					var label string
					if it.Valid() && bytes.Equal(it.Key(), vkey) {
						labelBytes, err := it.Value()
						if err != nil {
							return err
						}
						label = string(labelBytes)
					}
					localBatch.singles = append(localBatch.singles, vkey)
					if label != "" {
						select {
						case itemChan <- itemInfo{id: vid, label: label, isEdge: false, tbl: VTABLE_PREFIX + label}:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				select {
				case keyChan <- localBatch:
				case <-ctx.Done():
				}
			}
		}(slice)
	}

	// Prepare edge producers
	slices.Sort(data.Edges)
	edgeSlices := make([][]string, numCpus)
	for i, eid := range data.Edges {
		edgeSlices[i%numCpus] = append(edgeSlices[i%numCpus], eid)
	}
	for i := range edgeSlices {
		slices.Sort(edgeSlices[i])
	}

	for _, slice := range edgeSlices {
		if len(slice) == 0 {
			continue
		}
		prodWG.Add(1)
		go func(slice []string) {
			defer prodWG.Done()
			localBatch := keyBatch{singles: make([][]byte, 0, 12), ranges: make([][2][]byte, 0, 12)}

			err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, eid := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					if hasSeenEdge(eid) {
						continue
					}

					prefix := EdgeKeyPrefix(eid)
					if err := it.Seek(prefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
						nextPrefix := upperBound(prefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{prefix, nextPrefix})
						}
						var label string
						for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
							_, sid, did, lbl := EdgeKeyParse(it.Key())
							label = lbl
							localBatch.singles = append(localBatch.singles,
								SrcEdgeKey(eid, sid, did, lbl),
								DstEdgeKey(eid, sid, did, lbl))
							it.Next()
						}
						if label != "" {
							select {
							case itemChan <- itemInfo{id: eid, label: label, isEdge: true, tbl: ETABLE_PREFIX + label}:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				select {
				case keyChan <- localBatch:
				case <-ctx.Done():
				}
			}
		}(slice)
	}

	// Close channels and wait
	go func() {
		prodWG.Wait()
		close(itemChan)
	}()
	consWG.Wait()
	close(keyChan)
	aggWG.Wait()
	close(fieldChan)
	fieldWG.Wait()

	// Process field indices with single iterator
	var indexDelKeys [][]byte
	if len(allFields) > 0 {
		sort.Slice(allFields, func(i, j int) bool {
			return bytes.Compare(allFields[i].rKey, allFields[j].rKey) < 0
		})
		err := ggraph.jsonkv.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for _, fi := range allFields {
				if err := it.Seek(fi.rKey); err != nil {
					return err
				}
				if it.Valid() && bytes.Equal(it.Key(), fi.rKey) {
					valueBytes, err := it.Value()
					if err != nil {
						return err
					}
					var fieldValue any
					if err := sonic.ConfigFastest.Unmarshal(valueBytes, &fieldValue); err != nil {
						return err
					}
					if fieldValue != nil {
						fKey := benchtop.FieldKey(fi.field, fi.tbl, fieldValue, fi.id)
						indexDelKeys = append(indexDelKeys, fKey, fi.rKey)
					}
				}
			}
			return nil
		})
		addErr(err)
	}

	// Chunked deletes with Pebble batch
	chunked := func(singles [][]byte, ranges [][2][]byte, posKeys [][]byte, indexDelKeys [][]byte) error {
		batch := ggraph.jsonkv.Pkv.Db.NewBatch()
		defer batch.Close()
		for _, k := range singles {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		for _, r := range ranges {
			if err := batch.DeleteRange(r[0], r[1], nil); err != nil {
				return err
			}
		}
		for _, k := range posKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		for _, k := range indexDelKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	}

	// Perform deletes
	ggraph.jsonkv.PebbleLock.Lock()
	if err := chunked(singles, ranges, posKeys, indexDelKeys); err != nil {
		addErr(err)
	}
	ggraph.ts.Touch(ggraph.graphID)
	ggraph.jsonkv.PebbleLock.Unlock()

	log.Debugf("Total edges seen: %d", getSeenCount())
	return bulkErr.ErrorOrNil()
}

// upperBound computes the tight upper bound for range delete
func upperBound(prefix []byte) []byte {
	ub := make([]byte, len(prefix))
	copy(ub, prefix)
	for i := len(ub) - 1; i >= 0; i-- {
		if ub[i] < 0xFF {
			ub[i]++
			return ub[:i+1]
		}
	}
	return nil
}

// fnv32a computes FNV-1a 32-bit hash
func fnv32a(s string) uint32 {
	var h uint32 = 2166136261
	for i := range s {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
