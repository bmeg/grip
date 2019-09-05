package grids

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
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

// GetTimestamp returns the update timestamp
func (ggraph *GridsGraph) GetTimestamp() string {
	return ggraph.kdb.ts.Get(ggraph.graphID)
}

// Compiler gets a compiler that will use the graph the execute the compiled query
func (ggraph *GridsGraph) Compiler() gdbi.Compiler {
	return core.NewCompiler(ggraph)
}

type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *GridsGraph) AddVertex(vertices []*gripql.Vertex) error {
	dataChan := make(chan *kvAddData, 100)
	go func() {
		for _, vertex := range vertices {
			d, err := proto.Marshal(vertex)
			vertexKey := ggraph.kdb.keyMap.GetVertexKey(vertex.Gid)
			k := VertexKey(ggraph.graphKey, vertexKey)
			if err == nil {
				doc := map[string]interface{}{ggraph.graphID: vertexIdxStruct(vertex)}
				dataChan <- &kvAddData{key: k, value: d, vertex: vertex, doc: doc}
			}
		}
		close(dataChan)
	}()

	//TODO: split index out to other transation
	err := ggraph.kdb.graphkv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var anyErr error
		for kv := range dataChan {
			if err := tx.Set(kv.key, kv.value); err != nil {
				anyErr = err
				log.Errorf("AddVertex Error %s", err)
			} else {
				if err := ggraph.kdb.idx.AddDocTx(tx, kv.vertex.Gid, kv.doc); err != nil {
					anyErr = err
					log.Errorf("AddVertex Error %s", err)
				}
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
			var err error
			var data []byte

			data, err = proto.Marshal(edge)
			if err != nil {
				return err
			}

			label := ggraph.kdb.keyMap.GetLabelKey(edge.Label)
			eid := ggraph.kdb.keyMap.GetEdgeKey(edge.Gid) //TODO: fill in black key?
			src := ggraph.kdb.keyMap.GetVertexKey(edge.From)
			dst := ggraph.kdb.keyMap.GetVertexKey(edge.To)

			ekey := EdgeKey(ggraph.graphKey, eid, src, dst, label)
			skey := SrcEdgeKey(ggraph.graphKey, eid, src, dst, label)
			dkey := DstEdgeKey(ggraph.graphKey, eid, src, dst, label)

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
			//TODO: change this to a different TX
			err = ggraph.kdb.idx.AddDocTx(tx, edge.Gid, map[string]interface{}{ggraph.graphID: edgeIdxStruct(edge)})
			if err != nil {
				return err
			}
		}
		ggraph.kdb.ts.Touch(ggraph.graphID)
		return nil
	})
	return err
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

	ggraph.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(ggraph.graph, eid, sid, did, label)
			delKeys = append(delKeys, skey, ekey)
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, eid, sid, did, label := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(ggraph.graph, eid, sid, did, label)
			delKeys = append(delKeys, ekey)
		}
		return nil
	})

	return ggraph.kvg.kv.Update(func(tx kvi.KVTransaction) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		ggraph.kvg.ts.Touch(ggraph.graph)
		return nil
	})
}

// GetEdgeList produces a channel of all edges in the graph
func (kgdb *GridsGraph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			ePrefix := EdgeListPrefix(kgdb.graph)
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, eid, sid, did, label, etype := EdgeKeyParse(keyValue)
				if etype == edgeSingle {
					if loadProp {
						edgeData, _ := it.Value()
						e := &gripql.Edge{}
						proto.Unmarshal(edgeData, e)
						o <- e
					} else {
						e := &gripql.Edge{Gid: string(eid), Label: label, From: sid, To: did}
						o <- e
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (kgdb *GridsGraph) GetVertex(id string, loadProp bool) *gripql.Vertex {
	vkey := VertexKey(kgdb.graph, id)

	var v *gripql.Vertex
	err := kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
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
func (kgdb *GridsGraph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	data := make(chan elementData, 100)
	go func() {
		defer close(data)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for id := range ids {
				vkey := VertexKey(kgdb.graph, id.ID)
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
func (kgdb *GridsGraph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	go func() {
		defer close(vertexChan)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, _, _, dst, label := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						vkey := VertexKey(kgdb.graph, dst)
						if etype == edgeSingle {
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
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				dataValue, err := it.Get(req.data)
				if err == nil {
					_, gid := VertexKeyParse(req.data)
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
func (kgdb *GridsGraph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, _, src, _, label := DstEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						vkey := VertexKey(kgdb.graph, src)
						dataValue, err := it.Get(vkey)
						if err == nil {
							v := &gripql.Vertex{Gid: src}
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
func (kgdb *GridsGraph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, eid, src, dst, label := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if edgeType == edgeSingle {
							e := gripql.Edge{}
							if load {
								ekey := EdgeKey(kgdb.graph, eid, src, dst, label)
								dataValue, err := it.Get(ekey)
								if err == nil {
									proto.Unmarshal(dataValue, &e)
								}
							} else {
								e.Gid = string(eid)
								e.From = string(src)
								e.To = dst
								e.Label = label
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
func (kgdb *GridsGraph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, eid, src, dst, label := DstEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if edgeType == edgeSingle {
							e := gripql.Edge{}
							if load {
								ekey := EdgeKey(kgdb.graph, eid, src, dst, label)
								dataValue, err := it.Get(ekey)
								if err == nil {
									proto.Unmarshal(dataValue, &e)
								}
							} else {
								e.Gid = string(eid)
								e.From = string(src)
								e.To = dst
								e.Label = label
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
func (kgdb *GridsGraph) GetEdge(id string, loadProp bool) *gripql.Edge {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, id)

	var e *gripql.Edge
	err := kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
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
					Gid:   eid,
					From:  src,
					To:    dst,
					Label: label,
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
func (kgdb *GridsGraph) GetVertexList(ctx context.Context, loadProp bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			vPrefix := VertexListPrefix(kgdb.graph)

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
func (kgdb *GridsGraph) ListVertexLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.v.label", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (kgdb *GridsGraph) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.e.label", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}
