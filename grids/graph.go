package grids

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/protoutil"
	"github.com/bmeg/grip/util/setcmp"
	multierror "github.com/hashicorp/go-multierror"
)

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	//return ggraph.kdb.ts.Get(ggraph.graphID)
	return "" //FIXME
}

type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

func insertVertex(tx kvi.KVBulkWrite, keyMap *KeyMap, vertex *gdbi.Vertex) error {
	if vertex.ID == "" {
		return fmt.Errorf("Inserting null key vertex")
	}
	vertexKey, _ := keyMap.GetsertVertexKey(vertex.ID, vertex.Label)
	key := VertexKey(vertexKey)
	if vertex.Data == nil {
		vertex.Data = map[string]interface{}{}
	}
	value, err := protoutil.StructMarshal(vertex.Data)
	if err != nil {
		return err
	}
	if err := tx.Set(key, value); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func indexVertex(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, vertex *gdbi.Vertex) error {
	doc := vertexIdxStruct(vertex)
	if err := idx.AddDocTx(tx, vertex.ID, doc); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx kvi.KVBulkWrite, keyMap *KeyMap, edge *gdbi.Edge) error {
	var err error
	var data []byte

	if edge.ID == "" {
		return fmt.Errorf("inserting null key edge")
	}

	eid, lid := keyMap.GetsertEdgeKey(edge.ID, edge.Label)
	src, ok := keyMap.GetVertexKey(edge.From)
	if !ok {
		return fmt.Errorf("vertex %s not found", edge.From)
	}
	dst, ok := keyMap.GetVertexKey(edge.To)
	if !ok {
		return fmt.Errorf("vertex %s not found", edge.To)
	}

	ekey := EdgeKey(eid, src, dst, lid)
	skey := SrcEdgeKey(eid, src, dst, lid)
	dkey := DstEdgeKey(eid, src, dst, lid)

	data, err = protoutil.StructMarshal(edge.Data)
	if err != nil {
		return err
	}

	err = tx.Set(ekey, data)
	if err != nil {
		return err
	}
	err = tx.Set(skey, []byte{})
	if err != nil {
		return err
	}
	err = tx.Set(dkey, []byte{})
	if err != nil {
		return err
	}
	return nil
}

func indexEdge(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, edge *gdbi.Edge) error {
	err := idx.AddDocTx(tx, edge.ID, edgeIdxStruct(edge))
	return err
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	err := ggraph.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
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
	err = ggraph.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := indexVertex(tx, ggraph.idx, vert); err != nil {
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
	err := ggraph.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
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
	err = ggraph.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := indexEdge(tx, ggraph.idx, edge); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err

}

func (ggraph *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	var anyErr error
	insertStream := make(chan *gdbi.GraphElement, 100)
	indexStream := make(chan *gdbi.GraphElement, 100)
	s := &sync.WaitGroup{}
	s.Add(2)
	go func() {
		ggraph.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
			for elem := range insertStream {
				if elem.Vertex != nil {
					if err := insertVertex(tx, ggraph.keyMap, elem.Vertex); err != nil {
						anyErr = err
					}
				}
				if elem.Edge != nil {
					if err := insertEdge(tx, ggraph.keyMap, elem.Edge); err != nil {
						anyErr = err
					}
				}
			}
			s.Done()
			return anyErr
		})
	}()

	go func() {
		ggraph.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
			for elem := range indexStream {
				if elem.Vertex != nil {
					if err := indexVertex(tx, ggraph.idx, elem.Vertex); err != nil {
						anyErr = err
					}
				}
				if elem.Edge != nil {
					if err := indexEdge(tx, ggraph.idx, elem.Edge); err != nil {
						anyErr = err
					}
				}
			}
			s.Done()
			return anyErr
		})
	}()

	for i := range stream {
		insertStream <- i
		indexStream <- i
	}
	close(insertStream)
	close(indexStream)
	s.Wait()
	return anyErr
}

// DelEdge deletes edge with id `key`
func (ggraph *Graph) DelEdge(eid string) error {
	edgeKey, ok := ggraph.keyMap.GetEdgeKey(eid)
	if !ok {
		return fmt.Errorf("edge not found")
	}
	ekeyPrefix := EdgeKeyPrefix(edgeKey)
	var ekey []byte
	ggraph.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("edge not found")
	}

	_, sid, did, _ := EdgeKeyParse(ekey)

	skey := SrcEdgeKeyPrefix(edgeKey, sid, did)
	dkey := DstEdgeKeyPrefix(edgeKey, sid, did)

	var bulkErr *multierror.Error
	if err := ggraph.graphkv.Delete(ekey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.graphkv.Delete(skey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.graphkv.Delete(dkey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.keyMap.DelEdgeKey(eid); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	ggraph.ts.Touch(ggraph.graphID)
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

	err := ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			eid, sid, did, label := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(eid, sid, did, label)
			skey := SrcEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			edgeID, ok := ggraph.keyMap.GetEdgeID(eid)
			if ok {
				if err := ggraph.keyMap.DelEdgeKey(edgeID); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
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

	err = ggraph.graphkv.Update(func(tx kvi.KVTransaction) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k); err != nil {
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
					e.Data = map[string]interface{}{}
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
	err := ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
			v.Data = map[string]interface{}{}
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
					v.Data = map[string]interface{}{}
				}
				d.req.Vertex = &v
				out <- d.req
			}
		}
	}()

	return out
}

//GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					vertexChan <- elementData{req: req}
				} else {
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
							}
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				if req.req.IsSignal() {
					o <- req.req
				} else {
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
						v.Data = map[string]interface{}{}
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

//GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
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
									v.Data = map[string]interface{}{}
								}
								req.Vertex = v
								o <- req
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

//GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (ggraph *Graph) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
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
									e.Data = map[string]interface{}{}
								}
								req.Edge = &e
								o <- req
							}
						}
					}
				}
			}
			return nil
		})

	}()
	return o
}

//GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (ggraph *Graph) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					vkey, ok := ggraph.keyMap.GetVertexKey(req.ID)
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
									e.Data = map[string]interface{}{}
								}
								req.Edge = &e
								o <- req
							}
						}
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
	err := ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
				e.Data = map[string]interface{}{}
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
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
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
					v.Data = map[string]interface{}{}
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
	labelField := fmt.Sprintf("%s.v.label", ggraph.graphID)
	labels := []string{}
	for i := range ggraph.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.e.label", ggraph.graphID)
	labels := []string{}
	for i := range ggraph.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}
