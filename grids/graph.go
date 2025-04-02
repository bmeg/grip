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
	"github.com/bmeg/grip/util/protoutil"
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

func insertVertex(tx *pebblebulk.PebbleBulk, keyMap *KeyMap, vertex *gdbi.Vertex) error {
	if vertex.ID == "" {
		return fmt.Errorf("inserting null key vertex")
	}
	vertexKey, _ := keyMap.GetsertVertexKeyLabel(vertex.ID, vertex.Label, tx)
	key := VertexKey(vertexKey)
	if err := tx.Set(key, nil, nil); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func (ggraph *Graph) indexVertex(vertex *gdbi.Vertex) error {
	vertexLabel := VTABLE_PREFIX + vertex.Label
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[vertexLabel]
	ggraph.bsonkv.Lock.Unlock()
	if !ok {
		log.Debugf("Creating new table for: %s on graph %s", vertex.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(vertexLabel, nil)
		if err != nil {
			return fmt.Errorf("indexVertex: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[vertexLabel] = table
		ggraph.bsonkv.Lock.Unlock()
	}
	if err := table.AddRow(benchtop.Row{Id: []byte(vertex.ID), TableName: vertexLabel, Data: vertex.Data}); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx *pebblebulk.PebbleBulk, keyMap *KeyMap, edge *gdbi.Edge) error {
	var err error
	if edge.ID == "" {
		return fmt.Errorf("inserting null key edge")
	}

	eid, lid := keyMap.GetsertEdgeKey(edge.ID, edge.Label, tx)
	/* providing a label doesn't matter if not going to use the label key anyway.
	It can get set in the insertvertex func later */
	src := keyMap.GetsertVertexKey(edge.From, tx)
	dst := keyMap.GetsertVertexKey(edge.To, tx)

	ekey := EdgeKey(eid, src, dst, lid)
	skey := SrcEdgeKey(eid, src, dst, lid)
	dkey := DstEdgeKey(eid, src, dst, lid)

	err = tx.Set(ekey, nil, nil)
	if err != nil {
		return err
	}
	err = tx.Set(skey, []byte{}, nil)
	if err != nil {
		return err
	}
	err = tx.Set(dkey, []byte{}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (ggraph *Graph) indexEdge(edge *gdbi.Edge) error {
	edgeLabel := ETABLE_PREFIX + edge.Label
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[edgeLabel]
	ggraph.bsonkv.Lock.Unlock()

	if !ok {
		log.Debugf("Creating new table for: %s on graph %s", edge.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(edgeLabel, nil)
		if err != nil {
			return fmt.Errorf("indexEdge: bsonkv.New: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[edgeLabel] = table
		ggraph.bsonkv.Lock.Unlock()
	}
	if err := table.AddRow(benchtop.Row{Id: []byte(edge.ID), TableName: edgeLabel, Data: edge.Data}); err != nil {
		return fmt.Errorf("indexEdge: table.AddRow: %s", err)
	}
	return nil
}

func (ggraph *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(ggraph, core.IndexStartOptimize)
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, ggraph.keyMap, vert); err != nil {
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
			if err := ggraph.indexVertex(vert); err != nil {
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
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for _, edge := range edges {
			err := insertEdge(tx, ggraph.keyMap, edge)
			if err != nil {
				return err
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		return err
	}
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := ggraph.indexEdge(edge); err != nil {
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
	for _, val := range data.Edges {
		err := ggraph.DelEdge(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}
	for _, val := range data.Vertices {
		err := ggraph.DelVertex(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}
	return bulkErr.ErrorOrNil()
}

func (ggraph *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	var errs *multierror.Error
	insertStream := make(chan *gdbi.GraphElement, 100)
	indexStream := make(chan *benchtop.Row, 100)
	errChan := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine for inserting vertices and edges into graphkv
	go func() {
		defer wg.Done()
		err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for elem := range insertStream {
				if elem.Vertex != nil {
					if err := insertVertex(tx, ggraph.keyMap, elem.Vertex); err != nil {
						return fmt.Errorf("vertex insert error: %v", err)
					}
				}
				if elem.Edge != nil {
					if err := insertEdge(tx, ggraph.keyMap, elem.Edge); err != nil {
						return fmt.Errorf("edge insert error: %v", err)
					}
				}
			}
			ggraph.ts.Touch(ggraph.graphID)
			return nil
		})
		errChan <- err
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

	// Return any accumulated errors
	return errs.ErrorOrNil()
}

func (ggraph *Graph) DelEdge(eid string) error {
	edgeKey, ok := ggraph.keyMap.GetEdgeKey(eid, ggraph.bsonkv.Pb.Db)
	if !ok {
		return fmt.Errorf("edge not found")
	}
	ekeyPrefix := EdgeKeyPrefix(edgeKey)
	var ekey []byte
	ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})
	if ekey == nil {
		return fmt.Errorf("edge not found")
	}

	eidParsed, sid, did, lbl := EdgeKeyParse(ekey)

	skey := SrcEdgeKey(eidParsed, sid, did, lbl)
	dkey := DstEdgeKey(eidParsed, sid, did, lbl)

	var bulkErr *multierror.Error
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(ekey, nil); err != nil {
			return err
		}
		if err := tx.Delete(skey, nil); err != nil {
			return err
		}
		if err := tx.Delete(dkey, nil); err != nil {
			return err
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if err := ggraph.keyMap.DelEdgeKey(eid, ggraph.bsonkv.Pb.Db); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.bsonkv.DeleteAnyRow([]byte(eid)); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

// DelVertex deletes vertex with id `key`
func (ggraph *Graph) DelVertex(id string) error {
	vertexKey, ok := ggraph.keyMap.GetVertexKey(id, ggraph.bsonkv.Pb.Db)
	if !ok {
		return fmt.Errorf("vertex %s not found", id)
	}
	vid := VertexKey(vertexKey)
	skeyPrefix := SrcEdgePrefix(vertexKey)
	dkeyPrefix := DstEdgePrefix(vertexKey)

	delKeys := make([][]byte, 0, 1000)

	var bulkErr *multierror.Error

	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		var bulkErr *multierror.Error
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			eid, sid, did, label := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(eid, sid, did, label)
			dkey := DstEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			edgeID, ok := ggraph.keyMap.GetEdgeID(eid, ggraph.bsonkv.Pb.Db)
			if ok {
				if err := ggraph.keyMap.DelEdgeKey(edgeID, ggraph.bsonkv.Pb.Db); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
			if err := ggraph.bsonkv.DeleteAnyRow([]byte(edgeID)); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			eid, sid, did, label := DstEdgeKeyParse(dkey)
			ekey := EdgeKey(eid, sid, did, label)
			skey := SrcEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			edgeID, ok := ggraph.keyMap.GetEdgeID(eid, ggraph.bsonkv.Pb.Db)
			if ok {
				if err := ggraph.keyMap.DelEdgeKey(edgeID, ggraph.bsonkv.Pb.Db); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
			if err := ggraph.bsonkv.DeleteAnyRow([]byte(edgeID)); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		return bulkErr.ErrorOrNil()
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if err := ggraph.keyMap.DelVertexKey(id, ggraph.bsonkv.Pb.Db); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(vid, nil); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k, nil); err != nil {
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
				keyValue := it.Key()
				ekey, skey, dkey, label := EdgeKeyParse(keyValue)
				labelID, _ := ggraph.keyMap.GetLabelID(label, ggraph.bsonkv.Pb.Db)
				sid, _ := ggraph.keyMap.GetVertexID(skey, ggraph.bsonkv.Pb.Db)
				did, _ := ggraph.keyMap.GetVertexID(dkey, ggraph.bsonkv.Pb.Db)
				eid, _ := ggraph.keyMap.GetEdgeID(ekey, ggraph.bsonkv.Pb.Db)
				e := &gdbi.Edge{ID: eid, Label: labelID, From: sid, To: did}
				if loadProp {
					var err error
					e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+labelID].GetRow([]byte(eid))
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
	key, ok := ggraph.keyMap.GetVertexKey(id, ggraph.bsonkv.Pb.Db)
	if !ok {
		return nil
	}
	var v *gdbi.Vertex
	lKey := ggraph.keyMap.GetVertexLabel(key, ggraph.bsonkv.Pb.Db)
	lID, _ := ggraph.keyMap.GetLabelID(lKey, ggraph.bsonkv.Pb.Db)
	v = &gdbi.Vertex{
		ID:    id,
		Label: lID,
	}
	if loadProp {
		var err error
		v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+lID].GetRow([]byte(id))
		if err != nil {
			return nil
		}
		v.Loaded = true
	} else {
		v.Data = map[string]any{}
	}
	return v
}

type elementData struct {
	key  uint64
	req  gdbi.ElementLookup
	data []byte
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (ggraph *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	data := make(chan elementData, 100)
	go func() {
		defer close(data)
		for id := range ids {
			if id.IsSignal() {
				data <- elementData{req: id}
			} else {
				key, _ := ggraph.keyMap.GetVertexKey(id.ID, ggraph.bsonkv.Pb.Db)
				ed := elementData{key: key, req: id}
				if load {
					lKey := ggraph.keyMap.GetVertexLabel(key, ggraph.bsonkv.Pb.Db)
					lID, ok := ggraph.keyMap.GetLabelID(lKey, ggraph.bsonkv.Pb.Db)
					if !ok || lID == "" {
						log.Debugln("No LID for lkey: ", lKey)
						continue
					}
					vData, err := ggraph.bsonkv.Tables[VTABLE_PREFIX+lID].GetRow([]byte(id.ID))
					if err != nil {
						log.Errorf("GetVertexChannel: GetRow error for ID %s: %v", id.ID, err)
						continue
					}
					vdataM, _ := protoutil.StructMarshal(vData)
					ed.data = vdataM
				}
				data <- ed
			}
		}
	}()

	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for d := range data {
			if d.req.IsSignal() {
				out <- d.req
			} else {
				lKey := ggraph.keyMap.GetVertexLabel(d.key, ggraph.bsonkv.Pb.Db)
				lID, _ := ggraph.keyMap.GetLabelID(lKey, ggraph.bsonkv.Pb.Db)
				v := gdbi.Vertex{ID: d.req.ID, Label: lID}
				var err error
				v.Data, err = protoutil.StructUnMarshal(d.data)
				v.Loaded = true
				if err != nil {
					log.Errorf("GetVertexChannel: unmarshal error: %v", err)
					continue
				}

				d.req.Vertex = &v
				out <- d.req
			}
		}
	}()
	return out
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i], ggraph.bsonkv.Pb.Db)
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(vertexChan)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {

				if req.IsSignal() {
					vertexChan <- elementData{req: req}
				} else {
					found := false
					key, ok := ggraph.keyMap.GetVertexKey(req.ID, ggraph.bsonkv.Pb.Db)
					if ok {
						skeyPrefix := SrcEdgePrefix(key)
						for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
							keyValue := it.Key()
							_, _, dst, label := SrcEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								vkey := VertexKey(dst)
								vertexChan <- elementData{
									data: vkey,
									req:  req,
								}
								found = true
							}
						}
					}
					if !found && emitNull {
						vertexChan <- elementData{
							data: nil,
							req:  req,
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
		for req := range vertexChan {
			if req.req.IsSignal() {
				o <- req.req
			} else {
				if req.data == nil {
					req.req.Vertex = nil
					o <- req.req
					continue
				}
				vkey := VertexKeyParse(req.data)
				id, _ := ggraph.keyMap.GetVertexID(vkey, ggraph.bsonkv.Pb.Db)
				lkey := ggraph.keyMap.GetVertexLabel(vkey, ggraph.bsonkv.Pb.Db)
				lid, ok := ggraph.keyMap.GetLabelID(lkey, ggraph.bsonkv.Pb.Db)
				if !ok || lid == "" {
					log.Debugln("No LID for lkey: ", lkey)
					continue
				}
				v := &gdbi.Vertex{ID: id, Label: lid}
				var err error
				v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+lid].GetRow([]byte(id))
				v.Loaded = true
				if err != nil {
					log.Errorf("GetOutChannel: GetRow error: %v", err)
					continue
				}

				req.req.Vertex = v
				o <- req.req
			}
		}
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i], ggraph.bsonkv.Pb.Db)
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID, ggraph.bsonkv.Pb.Db)
					if ok {
						dkeyPrefix := DstEdgePrefix(vkey)
						for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
							keyValue := it.Key()
							_, src, _, label := DstEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								srcID, _ := ggraph.keyMap.GetVertexID(src, ggraph.bsonkv.Pb.Db)
								lKey := ggraph.keyMap.GetVertexLabel(src, ggraph.bsonkv.Pb.Db)
								lID, _ := ggraph.keyMap.GetLabelID(lKey, ggraph.bsonkv.Pb.Db)
								v := &gdbi.Vertex{ID: srcID, Label: lID}
								if load {
									var err error
									v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+lID].GetRow([]byte(srcID))
									if err != nil {
										log.Errorf("GetInChannel: GetRow error: %v", err)
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
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i], ggraph.bsonkv.Pb.Db)
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID, ggraph.bsonkv.Pb.Db)
					if ok {
						skeyPrefix := SrcEdgePrefix(vkey)
						for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
							keyValue := it.Key()
							eid, src, dst, label := SrcEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								e := gdbi.Edge{}
								e.ID, _ = ggraph.keyMap.GetEdgeID(eid, ggraph.bsonkv.Pb.Db)
								e.From, _ = ggraph.keyMap.GetVertexID(src, ggraph.bsonkv.Pb.Db)
								e.To, _ = ggraph.keyMap.GetVertexID(dst, ggraph.bsonkv.Pb.Db)
								e.Label, _ = ggraph.keyMap.GetLabelID(label, ggraph.bsonkv.Pb.Db)
								if load {
									var err error
									e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow([]byte(e.ID))
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
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i], ggraph.bsonkv.Pb.Db)
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID, ggraph.bsonkv.Pb.Db)
					found := false
					if ok {
						dkeyPrefix := DstEdgePrefix(vkey)
						for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
							keyValue := it.Key()
							eid, src, dst, label := DstEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								e := gdbi.Edge{}
								e.ID, _ = ggraph.keyMap.GetEdgeID(eid, ggraph.bsonkv.Pb.Db)
								e.From, _ = ggraph.keyMap.GetVertexID(src, ggraph.bsonkv.Pb.Db)
								e.To, _ = ggraph.keyMap.GetVertexID(dst, ggraph.bsonkv.Pb.Db)
								e.Label, _ = ggraph.keyMap.GetLabelID(label, ggraph.bsonkv.Pb.Db)
								if load {
									var err error
									e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+e.Label].GetRow([]byte(e.ID))
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
	ekey, ok := ggraph.keyMap.GetEdgeKey(id, ggraph.bsonkv.Pb.Db)
	if !ok {
		return nil
	}
	ekeyPrefix := EdgeKeyPrefix(ekey)

	var e *gdbi.Edge
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			eid, src, dst, labelKey := EdgeKeyParse(it.Key())
			id, _ := ggraph.keyMap.GetEdgeID(eid, ggraph.bsonkv.Pb.Db)
			from, _ := ggraph.keyMap.GetVertexID(src, ggraph.bsonkv.Pb.Db)
			to, _ := ggraph.keyMap.GetVertexID(dst, ggraph.bsonkv.Pb.Db)
			label, _ := ggraph.keyMap.GetLabelID(labelKey, ggraph.bsonkv.Pb.Db)
			e = &gdbi.Edge{
				ID:    id,
				From:  from,
				To:    to,
				Label: label,
			}
			if loadProp {
				var err error
				e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow([]byte(id))
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
				v := &gdbi.Vertex{}
				keyValue := it.Key()
				vKey := VertexKeyParse(keyValue)
				lKey := ggraph.keyMap.GetVertexLabel(vKey, ggraph.bsonkv.Pb.Db)
				v.ID, _ = ggraph.keyMap.GetVertexID(vKey, ggraph.bsonkv.Pb.Db)
				v.Label, _ = ggraph.keyMap.GetLabelID(lKey, ggraph.bsonkv.Pb.Db)
				if loadProp {
					var err error
					v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+v.Label].GetRow([]byte(v.ID))
					if err != nil {
						log.Errorf("GetVertexList: GetRow error: %v", err)
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
	for i := range ggraph.bsonkv.GetLabels(false) {
		labels = append(labels, i)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.bsonkv.GetLabels(true) {
		labels = append(labels, i)
	}
	return labels, nil
}
