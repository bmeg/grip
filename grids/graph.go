package grids

import (
	"bytes"
	"context"
	"sync"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/kvi"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}


func containsUint(a []uint64, v uint64) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

// GetTimestamp returns the update timestamp
func (ggraph *GridsGraph) GetTimestamp() string {
	return ggraph.kdb.ts.Get(ggraph.graphID)
}


type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

func insertVertex(tx kvi.KVBulkWrite, keyMap *KeyMap, graphKey uint64, vertex *gripql.Vertex) error {
	value, err := proto.Marshal(vertex)
	if err != nil {
		return err
	}
	vertexKey := keyMap.GetVertexKey(vertex.Gid)
	key := VertexKey(graphKey, vertexKey)
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

	data, err = proto.Marshal(edge)
	if err != nil {
		return err
	}

	label := keyMap.GetLabelKey(edge.Label)
	eid := keyMap.GetEdgeKey(edge.Gid) //TODO: fill in black key?
	src := keyMap.GetVertexKey(edge.From)
	dst := keyMap.GetVertexKey(edge.To)

	ekey := EdgeKey(graphKey, eid, src, dst, label)
	skey := SrcEdgeKey(graphKey, eid, src, dst, label)
	dkey := DstEdgeKey(graphKey, eid, src, dst, label)

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
func (ggraph *GridsGraph) AddVertex(vertices []*gripql.Vertex) error {
	err := ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var anyErr error
		for _, vert := range vertices {
			if err := insertVertex(tx, ggraph.kdb.keyMap, ggraph.graphKey, vert); err != nil {
				anyErr = err
				log.Errorf("AddVertex Error %s", err)
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return anyErr
	})
	err = ggraph.kdb.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var anyErr error
		for _, vert := range vertices {
			if err := indexVertex(tx, ggraph.kdb.idx, ggraph.graphID, vert); err != nil {
				anyErr = err
				log.Errorf("IndexVertex Error %s", err)
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return anyErr
	})
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (ggraph *GridsGraph) AddEdge(edges []*gripql.Edge) error {
	err := ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		for _, edge := range edges {
			insertEdge(tx, ggraph.kdb.keyMap, ggraph.graphKey, edge)
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return nil
	})
	err = ggraph.kdb.indexkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		for _, edge := range edges {
			indexEdge(tx, ggraph.kdb.idx, ggraph.graphID, edge)
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return nil
	})
	return err

}


func (ggraph *GridsGraph) BulkAdd(stream <-chan *gripql.GraphElement) error {
	var anyErr error
	insertStream := make(chan *gripql.GraphElement, 100)
	indexStream := make(chan *gripql.GraphElement, 100)
	s := &sync.WaitGroup{}
	s.Add(2)
	go func () {
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

	go func () {
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
func (ggraph *GridsGraph) DelEdge(eid string) error {
	edgeKey := ggraph.kdb.keyMap.GetEdgeKey(eid)
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

	if err := ggraph.kdb.graphkv.Delete(ekey); err != nil {
		return err
	}
	if err := ggraph.kdb.graphkv.Delete(skey); err != nil {
		return err
	}
	if err := ggraph.kdb.graphkv.Delete(dkey); err != nil {
		return err
	}
	ggraph.kdb.keyMap.DelEdgeKey(eid)
	ggraph.kdb.ts.Touch(ggraph.graphID)
	return nil
}

// DelVertex deletes vertex with id `key`
func (ggraph *GridsGraph) DelVertex(id string) error {
	vertexKey := ggraph.kdb.keyMap.GetVertexKey(id)
	vid := VertexKey(ggraph.graphKey, vertexKey)
	skeyPrefix := SrcEdgePrefix(ggraph.graphKey, vertexKey)
	dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vertexKey)

	delKeys := make([][]byte, 0, 1000)

	ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(ggraph.graphKey, eid, sid, did, label)
			delKeys = append(delKeys, skey, ekey)
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(ggraph.graphKey, eid, sid, did, label)
			delKeys = append(delKeys, ekey)
		}
		return nil
	})

	return ggraph.kdb.graphkv.Update(func(tx kvi.KVTransaction) error {
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
}

// GetEdgeList produces a channel of all edges in the graph
func (ggraph *GridsGraph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
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
				if loadProp {
					edgeData, _ := it.Value()
					e := &gripql.Edge{}
					proto.Unmarshal(edgeData, e)
					o <- e
				} else {
					labelID := ggraph.kdb.keyMap.GetLabelID(label)
					sid := ggraph.kdb.keyMap.GetVertexID(skey)
					did := ggraph.kdb.keyMap.GetVertexID(dkey)
					eid := ggraph.kdb.keyMap.GetEdgeID(ekey)
					e := &gripql.Edge{Gid: eid, Label: labelID, From: sid, To: did}
					o <- e
				}
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *GridsGraph) GetVertex(id string, loadProp bool) *gripql.Vertex {
	key := ggraph.kdb.keyMap.GetVertexKey(id)
	vkey := VertexKey(ggraph.graphKey, key)

	var v *gripql.Vertex
	err := ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		dataValue, err := it.Get(vkey)
		if err != nil {
			return fmt.Errorf("get call failed: %v", err)
		}
		v = &gripql.Vertex{
			Gid: id,
		}
		err = proto.Unmarshal(dataValue, v) //FIXME: this can't be skipped because vertex label is in value...
		if err != nil {
			return fmt.Errorf("unmarshal error: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return v
}

type elementData struct {
	req  gdbi.ElementLookup
	data []byte
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (ggraph *GridsGraph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	data := make(chan elementData, 100)
	go func() {
		defer close(data)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for id := range ids {
				key := ggraph.kdb.keyMap.GetVertexKey(id.ID)
				vkey := VertexKey(ggraph.graphKey, key)
				dataValue, err := it.Get(vkey)
				if err == nil {
					data <- elementData{
						req:  id,
						data: dataValue,
					}
				}
			}
			return nil
		})
	}()

	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for d := range data {
			v := gripql.Vertex{}
			proto.Unmarshal(d.data, &v)
			d.req.Vertex = &v
			out <- d.req
		}
	}()

	return out
}

//GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *GridsGraph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	edgeLabelKeys := make([]uint64, len(edgeLabels))
	for i := range edgeLabels {
		edgeLabelKeys[i] = ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
	}
	go func() {
		defer close(vertexChan)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				key := ggraph.kdb.keyMap.GetVertexKey(req.ID)
				skeyPrefix := SrcEdgePrefix(ggraph.graphKey, key)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, _, _, dst, label := SrcEdgeKeyParse(keyValue)
					if len(edgeLabelKeys) == 0 || containsUint(edgeLabelKeys, label) {
						vkey := VertexKey(ggraph.graphKey, dst)
						vertexChan <- elementData{
							data: vkey,
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
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				dataValue, err := it.Get(req.data)
				if err == nil {
					_, vkey := VertexKeyParse(req.data)
					gid := ggraph.kdb.keyMap.GetVertexID(vkey)
					v := &gripql.Vertex{Gid: gid}
					if load {
						err = proto.Unmarshal(dataValue, v)
						if err != nil {
							log.Errorf("GetOutChannel: unmarshal error: %v", err)
							continue
						}
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
func (ggraph *GridsGraph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelKeys := make([]uint64, len(edgeLabels))
	for i := range edgeLabels {
		edgeLabelKeys[i] = ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey := ggraph.kdb.keyMap.GetVertexKey(req.ID)
				dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vkey)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, _, src, _, label := DstEdgeKeyParse(keyValue)
					if len(edgeLabelKeys) == 0 || containsUint(edgeLabelKeys, label) {
						vkey := VertexKey(ggraph.graphKey, src)
						dataValue, err := it.Get(vkey)
						if err == nil {
							srcID := ggraph.kdb.keyMap.GetVertexID(src)
							v := &gripql.Vertex{Gid: srcID}
							if load {
								err = proto.Unmarshal(dataValue, v)
								if err != nil {
									log.Errorf("GetInChannel: unmarshal error: %v", err)
									continue
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
func (ggraph *GridsGraph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelKeys := make([]uint64, len(edgeLabels))
	for i := range edgeLabels {
		edgeLabelKeys[i] = ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey := ggraph.kdb.keyMap.GetVertexKey(req.ID)
				skeyPrefix := SrcEdgePrefix(ggraph.graphKey, vkey)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, eid, src, dst, label := SrcEdgeKeyParse(keyValue)
					if len(edgeLabelKeys) == 0 || containsUint(edgeLabelKeys, label) {
						e := gripql.Edge{}
						if load {
							ekey := EdgeKey(ggraph.graphKey, eid, src, dst, label)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, &e)
							}
						} else {
							e.Gid = ggraph.kdb.keyMap.GetEdgeID(eid)
							e.From = ggraph.kdb.keyMap.GetVertexID(src)
							e.To = ggraph.kdb.keyMap.GetVertexID(dst)
							e.Label = ggraph.kdb.keyMap.GetLabelID(label)
						}
						req.Edge = &e
						o <- req
					}
				}
			}
			return nil
		})

	}()
	return o
}

//GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (ggraph *GridsGraph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelKeys := make([]uint64, len(edgeLabels))
	for i := range edgeLabels {
		edgeLabelKeys[i] = ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				vkey := ggraph.kdb.keyMap.GetVertexKey(req.ID)
				dkeyPrefix := DstEdgePrefix(ggraph.graphKey, vkey)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, eid, src, dst, label := DstEdgeKeyParse(keyValue)
					if len(edgeLabelKeys) == 0 || containsUint(edgeLabelKeys, label) {
						e := gripql.Edge{}
						if load {
							ekey := EdgeKey(ggraph.graphKey, eid, src, dst, label)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, &e)
							}
						} else {
							e.Gid = ggraph.kdb.keyMap.GetEdgeID(eid)
							e.From = ggraph.kdb.keyMap.GetVertexID(src)
							e.To = ggraph.kdb.keyMap.GetVertexID(dst)
							e.Label = ggraph.kdb.keyMap.GetLabelID(label)
						}
						req.Edge = &e
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
func (ggraph *GridsGraph) GetEdge(id string, loadProp bool) *gripql.Edge {
	ekey := ggraph.kdb.keyMap.GetEdgeKey(id)
	ekeyPrefix := EdgeKeyPrefix(ggraph.graphKey, ekey)

	var e *gripql.Edge
	err := ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst, label := EdgeKeyParse(it.Key())
			if loadProp {
				e = &gripql.Edge{}
				d, _ := it.Value()
				err := proto.Unmarshal(d, e)
				if err != nil {
					return fmt.Errorf("unmarshal error: %v", err)
				}
			} else {
				e = &gripql.Edge{
					Gid:   ggraph.kdb.keyMap.GetEdgeID(eid),
					From:  ggraph.kdb.keyMap.GetVertexID(src),
					To:    ggraph.kdb.keyMap.GetVertexID(dst),
					Label: ggraph.kdb.keyMap.GetLabelID(label),
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
func (ggraph *GridsGraph) GetVertexList(ctx context.Context, loadProp bool) <-chan *gripql.Vertex {
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
				if loadProp {
					dataValue, _ := it.Value()
					proto.Unmarshal(dataValue, v)
				} else {
					keyValue := it.Key()
					_, vid := VertexKeyParse(keyValue)
					v.Gid = string(vid)
				}
				o <- v
			}
			return nil
		})
	}()
	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (ggraph *GridsGraph) ListVertexLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.v.label", ggraph.graphID)
	labels := []string{}
	for i := range ggraph.kdb.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *GridsGraph) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.e.label", ggraph.graphID)
	labels := []string{}
	for i := range ggraph.kdb.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}
