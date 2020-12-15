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
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util/setcmp"
	proto "github.com/golang/protobuf/proto"
	multierror "github.com/hashicorp/go-multierror"
)

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	return ggraph.kdb.ts.Get(ggraph.graphID)
}

type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

func insertVertex(tx kvi.KVBulkWrite, keyMap *KeyMap, graphKey uint64, vertex *gripql.Vertex) error {
	if vertex.Gid == "" {
		return fmt.Errorf("Inserting null key vertex")
	}
	vertexKey, _ := keyMap.GetsertVertexKey(graphKey, vertex.Gid, vertex.Label)
	key := VertexKey(graphKey, vertexKey)
	value, err := proto.Marshal(vertex.Data)
	if err != nil {
		return err
	}
	if err := tx.Set(key, value); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func indexVertex(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, vertex *gripql.Vertex) error {
	doc := map[string]interface{}{graph: vertexIdxStruct(vertex)}
	if err := idx.AddDocTx(tx, vertex.Gid, doc); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx kvi.KVBulkWrite, keyMap *KeyMap, graphKey uint64, edge *gripql.Edge) error {
	var err error
	var data []byte

	if edge.Gid == "" {
		return fmt.Errorf("Inserting null key edge")
	}

	eid, lid := keyMap.GetsertEdgeKey(graphKey, edge.Gid, edge.Label)
	src, ok := keyMap.GetVertexKey(graphKey, edge.From)
	if !ok {
		return fmt.Errorf("Vertex %s not found", edge.From)
	}
	dst, ok := keyMap.GetVertexKey(graphKey, edge.To)
	if !ok {
		return fmt.Errorf("Vertex %s not found", edge.To)
	}

	ekey := EdgeKey(graphKey, eid, src, dst, lid)
	skey := SrcEdgeKey(graphKey, eid, src, dst, lid)
	dkey := DstEdgeKey(graphKey, eid, src, dst, lid)

	data, err = proto.Marshal(edge.Data)
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

func indexEdge(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, edge *gripql.Edge) error {
	err := idx.AddDocTx(tx, edge.Gid, map[string]interface{}{graph: edgeIdxStruct(edge)})
	return err
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gripql.Vertex) error {
	err := ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, ggraph.kdb.keyMap, ggraph.graphKey, vert); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("AddVertex Error %s", err)
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	err = ggraph.kdb.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := indexVertex(tx, ggraph.kdb.idx, ggraph.graphID, vert); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("IndexVertex Error %s", err)
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (ggraph *Graph) AddEdge(edges []*gripql.Edge) error {
	err := ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		for _, edge := range edges {
			err := insertEdge(tx, ggraph.kdb.keyMap, ggraph.graphKey, edge)
			if err != nil {
				return err
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		return err
	}
	err = ggraph.kdb.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := indexEdge(tx, ggraph.kdb.idx, ggraph.graphID, edge); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err

}

func (ggraph *Graph) BulkAdd(stream <-chan *gripql.GraphElement) error {
	var anyErr error
	insertStream := make(chan *gripql.GraphElement, 100)
	indexStream := make(chan *gripql.GraphElement, 100)
	s := &sync.WaitGroup{}
	s.Add(2)
	go func() {
		ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
			for elem := range insertStream {
				if elem.Vertex != nil {
					if err := insertVertex(tx, ggraph.kdb.keyMap, ggraph.graphKey, elem.Vertex); err != nil {
						anyErr = err
					}
				}
				if elem.Edge != nil {
					if err := insertEdge(tx, ggraph.kdb.keyMap, ggraph.graphKey, elem.Edge); err != nil {
						anyErr = err
					}
				}
			}
			s.Done()
			return anyErr
		})
	}()

	go func() {
		ggraph.kdb.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
			for elem := range indexStream {
				if elem.Vertex != nil {
					if err := indexVertex(tx, ggraph.kdb.idx, ggraph.graphID, elem.Vertex); err != nil {
						anyErr = err
					}
				}
				if elem.Edge != nil {
					if err := indexEdge(tx, ggraph.kdb.idx, ggraph.graphID, elem.Edge); err != nil {
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
	edgeKey, ok := ggraph.kdb.keyMap.GetEdgeKey(ggraph.graphKey, eid)
	if !ok {
		fmt.Printf("Edge not found")
	}
	ekeyPrefix := EdgeKeyPrefix(ggraph.graphKey, edgeKey)
	var ekey []byte
	ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did, _ := EdgeKeyParse(ekey)

	skey := SrcEdgeKeyPrefix(ggraph.graphKey, edgeKey, sid, did)
	dkey := DstEdgeKeyPrefix(ggraph.graphKey, edgeKey, sid, did)

	var bulkErr *multierror.Error
	if err := ggraph.kdb.graphkv.Delete(ekey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.kdb.graphkv.Delete(skey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.kdb.graphkv.Delete(dkey); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.kdb.keyMap.DelEdgeKey(ggraph.graphKey, eid); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	ggraph.kdb.ts.Touch(ggraph.graphID)
	return bulkErr.ErrorOrNil()
}

// DelVertex deletes vertex with id `key`
func (ggraph *Graph) DelVertex(id string) error {
	vertexKey, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, id)
	if !ok {
		return fmt.Errorf("Vertex %s not found", id)
	}
	vid := VertexKey(ggraph.graphKey, vertexKey)
	skeyPrefix := SrcEdgePrefix(ggraph.graphKey, vertexKey)
	dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vertexKey)

	delKeys := make([][]byte, 0, 1000)

	var bulkErr *multierror.Error

	err := ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		var bulkErr *multierror.Error
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(ggraph.graphKey, eid, sid, did, label)
			delKeys = append(delKeys, skey, ekey)

			edgeID, ok := ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, eid)
			if ok {
				if err := ggraph.kdb.keyMap.DelEdgeKey(ggraph.graphKey, edgeID); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(ggraph.graphKey, eid, sid, did, label)
			delKeys = append(delKeys, ekey)

			edgeID, ok := ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, eid)
			if ok {
				if err := ggraph.kdb.keyMap.DelEdgeKey(ggraph.graphKey, edgeID); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
		}
		return bulkErr.ErrorOrNil()
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if err := ggraph.kdb.keyMap.DelVertexKey(ggraph.graphKey, id); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	err = ggraph.kdb.graphkv.Update(func(tx kvi.KVTransaction) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	return bulkErr.ErrorOrNil()
}

// GetEdgeList produces a channel of all edges in the graph
func (ggraph *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			ePrefix := EdgeListPrefix(ggraph.graphKey)
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, ekey, skey, dkey, label := EdgeKeyParse(keyValue)
				labelID, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, label)
				sid, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, skey)
				did, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, dkey)
				eid, _ := ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, ekey)
				e := &gripql.Edge{Gid: eid, Label: labelID, From: sid, To: did}
				if loadProp {
					edgeData, _ := it.Value()
					e.Data = protoutil.NewStruct()
					err := proto.Unmarshal(edgeData, e.Data)
					if err != nil {
						log.Errorf("GetEdgeList: unmarshal error: %v", err)
						continue
					}
				}
				o <- e
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *Graph) GetVertex(id string, loadProp bool) *gripql.Vertex {
	key, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, id)
	if !ok {
		return nil
	}
	vkey := VertexKey(ggraph.graphKey, key)

	var v *gripql.Vertex
	err := ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		lKey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, key)
		lID, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, lKey)
		v = &gripql.Vertex{
			Gid:   id,
			Label: lID,
		}
		if loadProp {
			dataValue, err := it.Get(vkey)
			v.Data = protoutil.NewStruct()
			err = proto.Unmarshal(dataValue, v.Data)
			if err != nil {
				return fmt.Errorf("unmarshal error: %v", err)
			}
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
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for id := range ids {
				key, _ := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, id.ID)
				ed := elementData{key: key, req: id}
				if load {
					vkey := VertexKey(ggraph.graphKey, key)
					dataValue, err := it.Get(vkey)
					if err == nil {
						ed.data = dataValue
					}
				}
				data <- ed
			}
			return nil
		})
	}()

	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for d := range data {
			lKey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, d.key)
			lID, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, lKey)
			v := gripql.Vertex{Gid: d.req.ID, Label: lID}
			if load {
				v.Data = protoutil.NewStruct()
				err := proto.Unmarshal(d.data, v.Data)
				if err != nil {
					log.Errorf("GetVertexChannel: unmarshal error: %v", err)
					continue
				}
			}
			d.req.Vertex = &v
			out <- d.req
		}
	}()

	return out
}

//GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(vertexChan)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				key, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, req.ID)
				if ok {
					skeyPrefix := SrcEdgePrefix(ggraph.graphKey, key)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						keyValue := it.Key()
						_, _, _, dst, label := SrcEdgeKeyParse(keyValue)
						if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
							vkey := VertexKey(ggraph.graphKey, dst)
							vertexChan <- elementData{
								data: vkey,
								req:  req,
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
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				_, vkey := VertexKeyParse(req.data)
				gid, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, vkey)
				lkey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, vkey)
				lid, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, lkey)
				v := &gripql.Vertex{Gid: gid, Label: lid}
				if load {
					dataValue, err := it.Get(req.data)
					if err == nil {
						v.Data = protoutil.NewStruct()
						err = proto.Unmarshal(dataValue, v.Data)
						if err != nil {
							log.Errorf("GetOutChannel: unmarshal error: %v", err)
							continue
						}
					}
				}
				req.req.Vertex = v
				o <- req.req
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
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, req.ID)
				if ok {
					dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vkey)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						keyValue := it.Key()
						_, _, src, _, label := DstEdgeKeyParse(keyValue)
						if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
							vkey := VertexKey(ggraph.graphKey, src)
							srcID, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, src)
							lID := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, src)
							lKey, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, lID)
							v := &gripql.Vertex{Gid: srcID, Label: lKey}
							if load {
								dataValue, err := it.Get(vkey)
								if err == nil {
									v.Data = protoutil.NewStruct()
									err = proto.Unmarshal(dataValue, v.Data)
									if err != nil {
										log.Errorf("GetInChannel: unmarshal error: %v", err)
										continue
									}
								}
							}
							req.Vertex = v
							o <- req
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
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, req.ID)
				if ok {
					skeyPrefix := SrcEdgePrefix(ggraph.graphKey, vkey)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						keyValue := it.Key()
						_, eid, src, dst, label := SrcEdgeKeyParse(keyValue)
						if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
							e := gripql.Edge{}
							e.Gid, _ = ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, eid)
							e.From, _ = ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, src)
							e.To, _ = ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, dst)
							e.Label, _ = ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, label)
							if load {
								ekey := EdgeKey(ggraph.graphKey, eid, src, dst, label)
								dataValue, err := it.Get(ekey)
								if err == nil {
									e.Data = protoutil.NewStruct()
									err := proto.Unmarshal(dataValue, e.Data)
									if err != nil {
										log.Errorf("GetOutEdgeChannel: unmarshal error: %v", err)
										continue
									}
								}
							}
							req.Edge = &e
							o <- req
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
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey, ok := ggraph.kdb.keyMap.GetVertexKey(ggraph.graphKey, req.ID)
				if ok {
					dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vkey)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						keyValue := it.Key()
						_, eid, src, dst, label := DstEdgeKeyParse(keyValue)
						if len(edgeLabelKeys) == 0 || setcmp.ContainsUint(edgeLabelKeys, label) {
							e := gripql.Edge{}
							e.Gid, _ = ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, eid)
							e.From, _ = ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, src)
							e.To, _ = ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, dst)
							e.Label, _ = ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, label)
							if load {
								ekey := EdgeKey(ggraph.graphKey, eid, src, dst, label)
								dataValue, err := it.Get(ekey)
								if err == nil {
									e.Data = protoutil.NewStruct()
									err := proto.Unmarshal(dataValue, e.Data)
									if err != nil {
										log.Errorf("GetInEdgeChannel: unmarshal error: %v", err)
										continue
									}
								}
							}
							req.Edge = &e
							o <- req
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
func (ggraph *Graph) GetEdge(id string, loadProp bool) *gripql.Edge {
	ekey, ok := ggraph.kdb.keyMap.GetEdgeKey(ggraph.graphKey, id)
	if !ok {
		return nil
	}
	ekeyPrefix := EdgeKeyPrefix(ggraph.graphKey, ekey)

	var e *gripql.Edge
	err := ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst, labelKey := EdgeKeyParse(it.Key())
			gid, _ := ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, eid)
			from, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, src)
			to, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, dst)
			label, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, labelKey)
			e = &gripql.Edge{
				Gid:   gid,
				From:  from,
				To:    to,
				Label: label,
			}
			if loadProp {
				d, _ := it.Value()
				e.Data = protoutil.NewStruct()
				err := proto.Unmarshal(d, e.Data)
				if err != nil {
					return fmt.Errorf("unmarshal error: %v", err)
				}
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
func (ggraph *Graph) GetVertexList(ctx context.Context, loadProp bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			vPrefix := VertexListPrefix(ggraph.graphKey)

			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				v := &gripql.Vertex{}
				keyValue := it.Key()
				_, vKey := VertexKeyParse(keyValue)
				lKey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, vKey)
				v.Gid, _ = ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, vKey)
				v.Label, _ = ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, lKey)
				if loadProp {
					dataValue, _ := it.Value()
					v.Data = protoutil.NewStruct()
					err := proto.Unmarshal(dataValue, v.Data)
					if err != nil {
						log.Errorf("GetVertexList: unmarshal error: %v", err)
						continue
					}
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
	for i := range ggraph.kdb.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.e.label", ggraph.graphID)
	labels := []string{}
	for i := range ggraph.kdb.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}
