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

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	return ggraph.ts.Get(ggraph.graphID)
}

func insertVertex(tx *pebblebulk.PebbleBulk, keyMap *KeyMap, vertex *gdbi.Vertex) error {
	if vertex.ID == "" {
		return fmt.Errorf("Inserting null key vertex")
	}
	vertexKey, _ := keyMap.GetsertVertexKeyLabel(vertex.ID, vertex.Label)
	key := VertexKey(vertexKey)
	if vertex.Data == nil {
		vertex.Data = map[string]any{}
	}
	value, err := protoutil.StructMarshal(vertex.Data)
	if err != nil {
		return err
	}
	if err := tx.Set(key, value, nil); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func (ggraph *Graph) indexVertex(vertex *gdbi.Vertex) error {
	vertexLabel := "v_" + vertex.Label
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

	if err := table.AddRow(benchtop.Row{Id: []byte(vertex.ID), TableName: vertexLabel, Data: vertexIdxStruct(vertex)}); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx *pebblebulk.PebbleBulk, keyMap *KeyMap, edge *gdbi.Edge) error {
	var err error
	var data []byte
	if edge.ID == "" {
		return fmt.Errorf("inserting null key edge")
	}

	eid, lid := keyMap.GetsertEdgeKey(edge.ID, edge.Label)
	/* providing a label doesn't matter if not going to use the label key anyway.
	It can get set in the insertvertex func later */
	src := keyMap.GetsertVertexKey(edge.From)
	dst := keyMap.GetsertVertexKey(edge.To)

	ekey := EdgeKey(eid, src, dst, lid)
	skey := SrcEdgeKey(eid, src, dst, lid)
	dkey := DstEdgeKey(eid, src, dst, lid)

	data, err = protoutil.StructMarshal(edge.Data)
	if err != nil {
		return err
	}

	err = tx.Set(ekey, data, nil)
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
	edgeLabel := "e_" + edge.Label
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
			if err := ggraph.bsonkv.BulkLoad(indexStream); err != nil {
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
					TableName: "v_" + elem.Vertex.Label,
					Data:      elem.Vertex.Data,
				}
			}
			if elem.Edge != nil {
				indexStream <- &benchtop.Row{
					Id:        []byte(elem.Edge.ID),
					TableName: "e_" + elem.Edge.Label,
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
	edgeKey, ok := ggraph.keyMap.GetEdgeKey(eid)
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

	if err := ggraph.keyMap.DelEdgeKey(eid); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.bsonkv.DeleteAnyRow([]byte(eid)); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

// DelVertex deletes vertex with id `key`
func (ggraph *Graph) DelVertex(id string) error {
	vertexKey, ok := ggraph.keyMap.GetVertexKey(id)
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

			edgeID, ok := ggraph.keyMap.GetEdgeID(eid)
			if ok {
				if err := ggraph.keyMap.DelEdgeKey(edgeID); err != nil {
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

			edgeID, ok := ggraph.keyMap.GetEdgeID(eid)
			if ok {
				if err := ggraph.keyMap.DelEdgeKey(edgeID); err != nil {
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

	if err := ggraph.keyMap.DelVertexKey(id); err != nil {
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
				labelID, _ := ggraph.keyMap.GetLabelID(label)
				sid, _ := ggraph.keyMap.GetVertexID(skey)
				did, _ := ggraph.keyMap.GetVertexID(dkey)
				eid, _ := ggraph.keyMap.GetEdgeID(ekey)
				e := &gdbi.Edge{ID: eid, Label: labelID, From: sid, To: did}

				if loadProp {
					var err error
					edgeData, _ := it.Value()
					e.Data, err = protoutil.StructUnMarshal(edgeData)
					e.Loaded = true
					if err != nil {
						log.Errorf("GetEdgeList: unmarshal error: %v", err)
						continue
					}
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
	key, ok := ggraph.keyMap.GetVertexKey(id)
	if !ok {
		return nil
	}
	vkey := VertexKey(key)

	var v *gdbi.Vertex
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		lKey := ggraph.keyMap.GetVertexLabel(key)
		lID, _ := ggraph.keyMap.GetLabelID(lKey)
		v = &gdbi.Vertex{
			ID:    id,
			Label: lID,
		}
		if loadProp {
			dataValue, err := it.Get(vkey)
			v.Data, err = protoutil.StructUnMarshal(dataValue)
			v.Loaded = true
			if err != nil {
				return fmt.Errorf("unmarshal error: %v", err)
			}
		} else {
			v.Data = map[string]any{}
		}
		return nil
	})
	if err != nil {
		return nil
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for id := range ids {
				if id.IsSignal() {
					data <- elementData{req: id}
				} else {
					key, _ := ggraph.keyMap.GetVertexKey(id.ID)
					ed := elementData{key: key, req: id}
					if load {
						vkey := VertexKey(key)
						dataValue, err := it.Get(vkey)
						if err == nil {
							ed.data = dataValue
						}
					}
					data <- ed
				}
			}
			return nil
		})
	}()

	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for d := range data {
			if d.req.IsSignal() {
				out <- d.req
			} else {
				lKey := ggraph.keyMap.GetVertexLabel(d.key)
				lID, _ := ggraph.keyMap.GetLabelID(lKey)
				v := gdbi.Vertex{ID: d.req.ID, Label: lID}
				if load {
					var err error
					v.Data, err = protoutil.StructUnMarshal(d.data)
					if err != nil {
						log.Errorf("GetVertexChannel: unmarshal error: %v", err)
						continue
					}
					v.Loaded = true
				} else {
					v.Data = map[string]any{}
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
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
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
					key, ok := ggraph.keyMap.GetVertexKey(req.ID)
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
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
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
					gid, _ := ggraph.keyMap.GetVertexID(vkey)
					lkey := ggraph.keyMap.GetVertexLabel(vkey)
					lid, _ := ggraph.keyMap.GetLabelID(lkey)
					v := &gdbi.Vertex{ID: gid, Label: lid}
					if load {
						dataValue, err := it.Get(req.data)
						if err == nil {
							v.Data, err = protoutil.StructUnMarshal(dataValue)
							if err != nil {
								log.Errorf("GetOutChannel: unmarshal error: %v", err)
								continue
							}
							v.Loaded = true
						}
					} else {
						v.Data = map[string]any{}
					}
					req.req.Vertex = v
					o <- req.req
				}
			}
			return nil
		})
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
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
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID)
					if ok {
						dkeyPrefix := DstEdgePrefix(vkey)
						for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
							keyValue := it.Key()
							_, src, _, label := DstEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								vkey := VertexKey(src)
								srcID, _ := ggraph.keyMap.GetVertexID(src)
								lKey := ggraph.keyMap.GetVertexLabel(src)
								lID, _ := ggraph.keyMap.GetLabelID(lKey)
								v := &gdbi.Vertex{ID: srcID, Label: lID}
								if load {
									dataValue, err := it.Get(vkey)
									if err == nil {
										v.Data, err = protoutil.StructUnMarshal(dataValue)
										if err != nil {
											log.Errorf("GetInChannel: unmarshal error: %v", err)
											continue
										}
										v.Loaded = true
									}
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
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
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
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID)
					if ok {
						skeyPrefix := SrcEdgePrefix(vkey)
						for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
							keyValue := it.Key()
							eid, src, dst, label := SrcEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								e := gdbi.Edge{}
								e.ID, _ = ggraph.keyMap.GetEdgeID(eid)
								e.From, _ = ggraph.keyMap.GetVertexID(src)
								e.To, _ = ggraph.keyMap.GetVertexID(dst)
								e.Label, _ = ggraph.keyMap.GetLabelID(label)
								if load {
									ekey := EdgeKey(eid, src, dst, label)
									dataValue, err := it.Get(ekey)
									if err == nil {
										e.Data, err = protoutil.StructUnMarshal(dataValue)
										if err != nil {
											log.Errorf("GetOutEdgeChannel: unmarshal error: %v", err)
											continue
										}
										e.Loaded = true
									}
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
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
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
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID)
					found := false
					if ok {
						dkeyPrefix := DstEdgePrefix(vkey)
						for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
							keyValue := it.Key()
							eid, src, dst, label := DstEdgeKeyParse(keyValue)
							if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
								e := gdbi.Edge{}
								e.ID, _ = ggraph.keyMap.GetEdgeID(eid)
								e.From, _ = ggraph.keyMap.GetVertexID(src)
								e.To, _ = ggraph.keyMap.GetVertexID(dst)
								e.Label, _ = ggraph.keyMap.GetLabelID(label)
								if load {
									ekey := EdgeKey(eid, src, dst, label)
									dataValue, err := it.Get(ekey)
									if err == nil {
										e.Data, err = protoutil.StructUnMarshal(dataValue)
										if err != nil {
											log.Errorf("GetInEdgeChannel: unmarshal error: %v", err)
											continue
										}
										e.Loaded = true
									}
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
	ekey, ok := ggraph.keyMap.GetEdgeKey(id)
	if !ok {
		return nil
	}
	ekeyPrefix := EdgeKeyPrefix(ekey)

	var e *gdbi.Edge
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			eid, src, dst, labelKey := EdgeKeyParse(it.Key())
			gid, _ := ggraph.keyMap.GetEdgeID(eid)
			from, _ := ggraph.keyMap.GetVertexID(src)
			to, _ := ggraph.keyMap.GetVertexID(dst)
			label, _ := ggraph.keyMap.GetLabelID(labelKey)
			e = &gdbi.Edge{
				ID:    gid,
				From:  from,
				To:    to,
				Label: label,
			}
			if loadProp {
				var err error
				d, _ := it.Value()
				e.Data, err = protoutil.StructUnMarshal(d)
				if err != nil {
					return fmt.Errorf("unmarshal error: %v", err)
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
				lKey := ggraph.keyMap.GetVertexLabel(vKey)
				v.ID, _ = ggraph.keyMap.GetVertexID(vKey)
				v.Label, _ = ggraph.keyMap.GetLabelID(lKey)
				if loadProp {
					var err error
					dataValue, _ := it.Value()
					v.Data, err = protoutil.StructUnMarshal(dataValue)
					if err != nil {
						log.Errorf("GetVertexList: unmarshal error: %v", err)
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
