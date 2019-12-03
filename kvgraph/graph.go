package kvgraph

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/log"
	proto "github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-multierror"
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
func (kgdb *KVInterfaceGDB) GetTimestamp() string {
	return kgdb.kvg.ts.Get(kgdb.graph)
}

// Compiler gets a compiler that will use the graph the execute the compiled query
func (kgdb *KVInterfaceGDB) Compiler() gdbi.Compiler {
	return core.NewCompiler(kgdb)
}

type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (kgdb *KVInterfaceGDB) AddVertex(vertices []*gripql.Vertex) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, kgdb.kvg.idx, kgdb.graph, vert); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return bulkErr.ErrorOrNil()
	})
	return err
}

func insertVertex(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, vertex *gripql.Vertex) error {
	if err := vertex.Validate(); err != nil {
		return err
	}

	key := VertexKey(graph, vertex.Gid)
	value, err := proto.Marshal(vertex)
	if err != nil {
		return nil
	}
	doc := map[string]interface{}{graph: vertexIdxStruct(vertex)}
	if err := tx.Set(key, value); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	if err := idx.AddDocTx(tx, vertex.Gid, doc); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func insertEdge(tx kvi.KVBulkWrite, idx *kvindex.KVIndex, graph string, edge *gripql.Edge) error {
	eid := edge.Gid
	var err error
	var data []byte

	if err = edge.Validate(); err != nil {
		return err
	}

	data, err = proto.Marshal(edge)
	if err != nil {
		return err
	}

	src := edge.From
	dst := edge.To
	ekey := EdgeKey(graph, eid, src, dst, edge.Label, edgeSingle)
	skey := SrcEdgeKey(graph, src, dst, eid, edge.Label, edgeSingle)
	dkey := DstEdgeKey(graph, src, dst, eid, edge.Label, edgeSingle)

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
	err = idx.AddDocTx(tx, eid, map[string]interface{}{graph: edgeIdxStruct(edge)})
	if err != nil {
		return err
	}
	return nil
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (kgdb *KVInterfaceGDB) AddEdge(edges []*gripql.Edge) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := insertEdge(tx, kgdb.kvg.idx, kgdb.graph, edge); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return bulkErr.ErrorOrNil()
	})
	return err
}

func (kgdb *KVInterfaceGDB) BulkAdd(stream <-chan *gripql.GraphElement) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for elem := range stream {
			if elem.Vertex != nil {
				if err := insertVertex(tx, kgdb.kvg.idx, kgdb.graph, elem.Vertex); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
				continue
			}
			if elem.Edge != nil {
				if err := insertEdge(tx, kgdb.kvg.idx, kgdb.graph, elem.Edge); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
				continue
			}
		}
		return bulkErr.ErrorOrNil()
	})
	return err
}

// DelEdge deletes edge with id `key`
func (kgdb *KVInterfaceGDB) DelEdge(eid string) error {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, eid)
	var ekey []byte
	kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did, _, _ := EdgeKeyParse(ekey)

	skey := SrcEdgeKeyPrefix(kgdb.graph, sid, did, eid)
	dkey := DstEdgeKeyPrefix(kgdb.graph, sid, did, eid)

	if err := kgdb.kvg.kv.Delete(ekey); err != nil {
		return err
	}
	if err := kgdb.kvg.kv.Delete(skey); err != nil {
		return err
	}
	if err := kgdb.kvg.kv.Delete(dkey); err != nil {
		return err
	}
	kgdb.kvg.ts.Touch(kgdb.graph)
	return nil
}

// DelVertex deletes vertex with id `key`
func (kgdb *KVInterfaceGDB) DelVertex(id string) error {
	vid := VertexKey(kgdb.graph, id)
	skeyPrefix := SrcEdgePrefix(kgdb.graph, id)
	dkeyPrefix := DstEdgePrefix(kgdb.graph, id)

	delKeys := make([][]byte, 0, 1000)

	kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			_, sid, did, eid, label, etype := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(kgdb.graph, eid, sid, did, label, etype)
			delKeys = append(delKeys, skey, ekey)
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, sid, did, eid, label, etype := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(kgdb.graph, eid, sid, did, label, etype)
			delKeys = append(delKeys, ekey)
		}
		return nil
	})

	return kgdb.kvg.kv.Update(func(tx kvi.KVTransaction) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return nil
	})
}

// GetEdgeList produces a channel of all edges in the graph
func (kgdb *KVInterfaceGDB) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
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
func (kgdb *KVInterfaceGDB) GetVertex(id string, loadProp bool) *gripql.Vertex {
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
func (kgdb *KVInterfaceGDB) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
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
func (kgdb *KVInterfaceGDB) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	go func() {
		defer close(vertexChan)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, _, dst, _, label, etype := SrcEdgeKeyParse(keyValue)
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
func (kgdb *KVInterfaceGDB) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, src, _, _, label, _ := DstEdgeKeyParse(keyValue)
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
func (kgdb *KVInterfaceGDB) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, src, dst, eid, label, edgeType := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if edgeType == edgeSingle {
							e := gripql.Edge{}
							if load {
								ekey := EdgeKey(kgdb.graph, eid, src, dst, label, edgeType)
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
func (kgdb *KVInterfaceGDB) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					keyValue := it.Key()
					_, src, dst, eid, label, edgeType := DstEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if edgeType == edgeSingle {
							e := gripql.Edge{}
							if load {
								ekey := EdgeKey(kgdb.graph, eid, src, dst, label, edgeType)
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
func (kgdb *KVInterfaceGDB) GetEdge(id string, loadProp bool) *gripql.Edge {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, id)

	var e *gripql.Edge
	err := kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst, label, _ := EdgeKeyParse(it.Key())
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
func (kgdb *KVInterfaceGDB) GetVertexList(ctx context.Context, loadProp bool) <-chan *gripql.Vertex {
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
func (kgdb *KVInterfaceGDB) ListVertexLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.v.label", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (kgdb *KVInterfaceGDB) ListEdgeLabels() ([]string, error) {
	labelField := fmt.Sprintf("%s.e.label", kgdb.graph)
	labels := []string{}
	for i := range kgdb.kvg.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}
	return labels, nil
}
