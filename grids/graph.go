package grids

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/setcmp"
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
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[vertexLabel]
	ggraph.bsonkv.Lock.Unlock()
	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", vertexLabel, vertex.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(vertexLabel, nil)
		if err != nil {
			return fmt.Errorf("indexVertex: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[vertexLabel] = table
		ggraph.bsonkv.Lock.Unlock()
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
	table.AddTableEntryInfo(tx, []byte(vertex.ID), *rowLoc)

	_, ok = ggraph.bsonkv.PageCache.Set(vertex.ID, *rowLoc)
	if !ok {
		ggraph.bsonkv.PageCache.Invalidate(vertex.ID)
		ggraph.bsonkv.PageCache.Set(vertex.ID, *rowLoc)
		//log.Debugln("Replaced vals: ", vertex.ID, oldVal, newVal)
	}

	_, fieldsExist := ggraph.bsonkv.Fields[vertexLabel]
	if fieldsExist {
		for field := range ggraph.bsonkv.Fields[vertexLabel] {
			if val := bsontable.PathLookup(vertex.Data, field); val != nil {
				tx.Set(benchtop.FieldKey(field, vertexLabel, val, []byte(vertex.ID)), []byte{}, nil)
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
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[edgeLabel]
	ggraph.bsonkv.Lock.Unlock()

	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", edgeLabel, edge.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(edgeLabel, nil)
		if err != nil {
			return fmt.Errorf("indexEdge: bsonkv.New: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[edgeLabel] = table
		ggraph.bsonkv.Lock.Unlock()
	}
	rowLoc, err := table.AddRow(benchtop.Row{Id: []byte(edge.ID), TableName: edgeLabel, Data: edge.Data})
	if err != nil {
		return fmt.Errorf("indexEdge: table.AddRow: %s", err)
	}
	table.AddTableEntryInfo(tx, []byte(edge.ID), *rowLoc)

	_, ok = ggraph.bsonkv.PageCache.Set(edge.ID, *rowLoc)
	if !ok {
		ggraph.bsonkv.PageCache.Invalidate(edge.ID)
		ggraph.bsonkv.PageCache.Set(edge.ID, *rowLoc)
	}

	_, fieldsExist := ggraph.bsonkv.Fields[edgeLabel]
	if fieldsExist {
		for field := range ggraph.bsonkv.Fields[edgeLabel] {
			if val := bsontable.PathLookup(edge.Data, field); val != nil {
				tx.Set(benchtop.FieldKey(field, edgeLabel, val, []byte(edge.ID)), []byte{}, nil)
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
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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

	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		err = ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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

func (ggraph *Graph) BulkDel(data *gdbi.DeleteData) error {
	var bulkErr *multierror.Error
	ggraph.tempDeletedEdges = make(map[string]struct{})
	ggraph.edgesMutex.Lock()

	for _, val := range data.Vertices {
		err := ggraph.DelVertex(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}

	for _, val := range data.Edges {
		if _, ok := ggraph.tempDeletedEdges[val]; ok {
			log.Debugf("Skipping edge %s: already deleted during vertex deletion", val)
			continue
		}
		err := ggraph.DelEdge(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}

	ggraph.tempDeletedEdges = nil // Clean up
	defer ggraph.edgesMutex.Unlock()
	return bulkErr.ErrorOrNil()
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
		err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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
		err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			if err := ggraph.bsonkv.BulkLoad(indexStream, tx); err != nil {
				return fmt.Errorf("bsonkv bulk load error: %v", err)
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

	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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

	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
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

// GetEdgeList produces a channel of all edges in the graph
func (ggraph *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gdbi.Edge {
	o := make(chan *gdbi.Edge, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			ePrefix := EdgeListPrefix()
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				eid, sid, did, label := EdgeKeyParse(it.Key())
				e := &gdbi.Edge{ID: eid, Label: label, From: sid, To: did}
				if loadProp {
					entry, err := ggraph.bsonkv.PageCache.Get(ctx, eid, ggraph.bsonkv.PageLoader)
					if err != nil {
						log.Errorf("GetEdgeList: PageCache.Get( error: %v", err)
						continue
					}
					e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow(entry)
					if err != nil {
						log.Errorf("GetEdgeList: GetRow error: %v", err)
						continue
					}
					e.Loaded = true
				} else {
					e.Data = map[string]any{}
				}
				o <- e
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *Graph) GetVertex(id string, loadProp bool) *gdbi.Vertex {
	ekeyPrefix := VertexKey(id)
	var byteLabel []byte = nil
	var err error = nil
	err = ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
		entry, err := ggraph.bsonkv.PageCache.Get(context.Background(), id, ggraph.bsonkv.PageLoader)
		if err != nil {
			log.Errorf("GetVertex: PageCache.Get( error: %v", err)
			return nil
		}
		v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
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

func (ggraph *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for id := range ids {
				if id.IsSignal() {
					out <- id
				} else {
					if load {
						prefix := VertexKey(id.ID)
						v := gdbi.Vertex{ID: id.ID}
						for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
							label, err := it.Value()
							if err != nil {
								log.Errorln("GetVertexChannel it.Value() err: ", err)
								continue
							}
							v.Label = string(label)

							entry, err := ggraph.bsonkv.PageCache.Get(ctx, id.ID, ggraph.bsonkv.PageLoader)
							if err != nil {
								log.Errorf("GetVertexChannel: PageCache.Get( error: %v", err)
								continue
							}
							v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
							if err != nil {
								log.Errorf("GetVertexChannel: GetRow error for ID %s: %v", id.ID, err)
								continue
							}
							v.Loaded = true
						}
						id.Vertex = &v
						out <- id

					} else {
						id.Vertex = &gdbi.Vertex{ID: id.ID}
						out <- id
					}
				}
			}
			return nil
		})
	}()
	return out
}

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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					lookupChan <- lookup{req: req}
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
				o <- req.req
			} else {
				if req.key != "" {
					entry, err := ggraph.bsonkv.PageCache.Get(ctx, req.key, ggraph.bsonkv.PageLoader)
					if err != nil {
						log.Errorf("GetOutChannel: PageCache.Get( error: %v", err)
						continue
					}
					vLabel, ok := ggraph.bsonkv.LabelLookup[entry.Label]
					if !ok {
						log.Errorf("GetOutChannel: Label not a string %s", vLabel)
						continue
					}
					v := &gdbi.Vertex{ID: req.key, Label: vLabel}
					if load {
						v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
						if err != nil {
							log.Errorf("GetOutChannel: GetRow on %s: %s error: %v", vLabel, req.key, err)
							continue
						}
						v.Loaded = true
					}else {
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						_, sid, _, label := DstEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							entry, err := ggraph.bsonkv.PageCache.Get(ctx, sid, ggraph.bsonkv.PageLoader)
							if err != nil {
								log.Errorf("GetInChannel: PageCache.Get( error: %v", err)
								continue
							}

							vLabel, ok := ggraph.bsonkv.LabelLookup[entry.Label]
							if !ok {
								log.Errorf("GetInChannel Label lookup failed")
								continue
							}

							v := &gdbi.Vertex{ID: sid, Label: vLabel}
							if load {
								v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
								entry, err := ggraph.bsonkv.PageCache.Get(ctx, e.ID, ggraph.bsonkv.PageLoader)
								if err != nil {
									log.Errorf("GetOutEdgeChannel: PageCache.Get( error: %v", err)
									continue
								}
								e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
								entry, err := ggraph.bsonkv.PageCache.Get(ctx, e.ID, ggraph.bsonkv.PageLoader)
								if err != nil {
									log.Errorf("GetInEdgeChannel: PageCache.Get( error: %v", err)
									continue
								}
								//log.Debugln("IN EDGE LABEL: ", e.Label, "ENTRY: ", entry, "ID: ", e.ID)

								e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
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
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			eid, src, dst, label := EdgeKeyParse(it.Key())
			e = &gdbi.Edge{
				ID:    eid,
				From:  src,
				To:    dst,
				Label: label,
			}
			if loadProp {
				entry, err := ggraph.bsonkv.PageCache.Get(context.Background(), e.ID, ggraph.bsonkv.PageLoader)
				if err != nil {
					log.Errorf("GetEdge: PageCache.Get( error: %v", err)
					continue
				}

				e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow(entry)
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
					entry, err := ggraph.bsonkv.PageCache.Get(context.Background(), v.ID, ggraph.bsonkv.PageLoader)
					if err != nil {
						log.Errorf("GetVertexList: PageCache.Get on %s error: %s", v.ID, err)
						continue
					}

					v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow(entry)
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
	for i := range ggraph.bsonkv.GetLabels(false, true) {
		labels = append(labels, i)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.bsonkv.GetLabels(true, true) {
		labels = append(labels, i)
	}
	return labels, nil
}
