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
	"google.golang.org/protobuf/proto"

	multierror "github.com/hashicorp/go-multierror"
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
	return core.NewCompiler(kgdb, core.IndexStartOptimize)
}

type kvAddData struct {
	key    []byte
	value  []byte
	vertex *gripql.Vertex
	doc    map[string]interface{}
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (kgdb *KVInterfaceGDB) AddVertex(vertices []*gdbi.Vertex) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, kgdb.kvg.idx, kgdb.graph, vert.ToVertex()); err != nil {
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
func (kgdb *KVInterfaceGDB) AddEdge(edges []*gdbi.Edge) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := insertEdge(tx, kgdb.kvg.idx, kgdb.graph, edge.ToEdge()); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return bulkErr.ErrorOrNil()
	})
	return err
}

func (kgdb *KVInterfaceGDB) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	err := kgdb.kvg.kv.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var bulkErr *multierror.Error
		for elem := range stream {
			if elem.Vertex != nil {
				if err := insertVertex(tx, kgdb.kvg.idx, kgdb.graph, elem.Vertex.ToVertex()); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
				continue
			}
			if elem.Edge != nil {
				if err := insertEdge(tx, kgdb.kvg.idx, kgdb.graph, elem.Edge.ToEdge()); err != nil {
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
			dkey := DstEdgeKey(kgdb.graph, sid, did, eid, label, etype)
			delKeys = append(delKeys, skey, dkey, ekey)
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, sid, did, eid, label, etype := DstEdgeKeyParse(dkey)
			ekey := EdgeKey(kgdb.graph, eid, sid, did, label, etype)
			skey := SrcEdgeKey(kgdb.graph, sid, did, eid, label, etype)
			delKeys = append(delKeys, skey, dkey, ekey)
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
func (kgdb *KVInterfaceGDB) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gdbi.Edge {
	o := make(chan *gdbi.Edge, 100)
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
						ge := &gripql.Edge{}
						proto.Unmarshal(edgeData, ge)
						e := &gdbi.Edge{ID: ge.Gid, Label: ge.Label, From: sid, To: did, Data: ge.Data.AsMap(), Loaded: true}
						o <- e
					} else {
						e := &gdbi.Edge{ID: string(eid), Label: label, From: sid, To: did, Loaded: false}
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
func (kgdb *KVInterfaceGDB) GetVertex(id string, loadProp bool) *gdbi.Vertex {
	vkey := VertexKey(kgdb.graph, id)

	var v *gdbi.Vertex
	err := kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		dataValue, err := it.Get(vkey)
		if err != nil {
			return fmt.Errorf("get call failed: %v", err)
		}
		gv := &gripql.Vertex{
			Gid: id,
		}
		err = proto.Unmarshal(dataValue, gv) //FIXME: this can't be skipped because vertex label is in value...
		if err != nil {
			return fmt.Errorf("unmarshal error: %v", err)
		}
		v = &gdbi.Vertex{
			ID:     id,
			Label:  gv.Label,
			Data:   gv.Data.AsMap(),
			Loaded: true,
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
func (kgdb *KVInterfaceGDB) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	data := make(chan elementData, 100)
	go func() {
		defer close(data)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for id := range ids {
				if id.IsSignal() {
					data <- elementData{req: id}
				} else {
					vkey := VertexKey(kgdb.graph, id.ID)
					dataValue, err := it.Get(vkey)
					if err == nil {
						data <- elementData{
							req:  id,
							data: dataValue,
						}
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
			if d.req.IsSignal() {
				out <- d.req
			} else {
				v := gripql.Vertex{}
				proto.Unmarshal(d.data, &v)
				d.req.Vertex = &gdbi.Vertex{
					ID:     d.req.ID,
					Label:  v.Label,
					Data:   v.Data.AsMap(),
					Loaded: true,
				}
				out <- d.req
			}
		}
	}()

	return out
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (kgdb *KVInterfaceGDB) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	vertexChan := make(chan elementData, 100)
	go func() {
		defer close(vertexChan)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					vertexChan <- elementData{req: req}
				} else {
					found := false
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
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				if req.req.IsSignal() {
					o <- req.req
				} else {
					if req.data != nil {
						dataValue, err := it.Get(req.data)
						if err == nil {
							_, gid := VertexKeyParse(req.data)
							v := &gripql.Vertex{Gid: gid}
							//if load { //TODO: can't skip loading data, because the label in the data
							err = proto.Unmarshal(dataValue, v)
							if err != nil {
								log.Errorf("GetOutChannel: unmarshal error: %v", err)
								continue
								//}
							}
							req.req.Vertex = &gdbi.Vertex{
								ID:     gid,
								Label:  v.Label,
								Data:   v.Data.AsMap(),
								Loaded: true,
							}
							o <- req.req
						}
					} else {
						req.req.Vertex = nil
						o <- req.req
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (kgdb *KVInterfaceGDB) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						keyValue := it.Key()
						_, src, _, _, label, _ := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || contains(edgeLabels, label) {
							vkey := VertexKey(kgdb.graph, src)
							dataValue, err := it.Get(vkey)
							if err == nil {
								v := &gripql.Vertex{Gid: src}
								//if load { //TODO: Can't skip data load because vertex label is in data
								err = proto.Unmarshal(dataValue, v)
								if err != nil {
									log.Errorf("GetInChannel: unmarshal error: %v", err)
									continue
								}
								//}
								req.Vertex = &gdbi.Vertex{
									ID:     src,
									Label:  v.Label,
									Data:   v.Data.AsMap(),
									Loaded: true,
								}
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
func (kgdb *KVInterfaceGDB) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						keyValue := it.Key()
						_, src, dst, eid, label, edgeType := SrcEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || contains(edgeLabels, label) {
							if edgeType == edgeSingle {
								e := gdbi.Edge{}
								if load {
									ekey := EdgeKey(kgdb.graph, eid, src, dst, label, edgeType)
									dataValue, err := it.Get(ekey)
									ge := gripql.Edge{}
									if err == nil {
										proto.Unmarshal(dataValue, &ge)
										e.ID = string(eid)
										e.From = string(src)
										e.To = dst
										e.Label = label
										e.Data = ge.Data.AsMap()
										e.Loaded = true
									}
								} else {
									e.ID = string(eid)
									e.From = string(src)
									e.To = dst
									e.Label = label
									e.Loaded = false
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
func (kgdb *KVInterfaceGDB) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					dkeyPrefix := DstEdgePrefix(kgdb.graph, req.ID)
					found := false
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						keyValue := it.Key()
						_, src, dst, eid, label, edgeType := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || contains(edgeLabels, label) {
							if edgeType == edgeSingle {
								e := gdbi.Edge{}
								if load {
									ekey := EdgeKey(kgdb.graph, eid, src, dst, label, edgeType)
									dataValue, err := it.Get(ekey)
									if err == nil {
										ge := gripql.Edge{}
										proto.Unmarshal(dataValue, &ge)
										e.ID = string(eid)
										e.From = string(src)
										e.To = dst
										e.Label = label
										e.Data = ge.Data.AsMap()
										e.Loaded = true
									}
								} else {
									e.ID = string(eid)
									e.From = string(src)
									e.To = dst
									e.Label = label
									e.Loaded = false
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
func (kgdb *KVInterfaceGDB) GetEdge(id string, loadProp bool) *gdbi.Edge {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, id)

	var e *gdbi.Edge
	err := kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst, label, _ := EdgeKeyParse(it.Key())
			if loadProp {
				d, _ := it.Value()
				ge := &gripql.Edge{}
				err := proto.Unmarshal(d, ge)
				if err != nil {
					return fmt.Errorf("unmarshal error: %v", err)
				}
				e = &gdbi.Edge{
					ID:     eid,
					From:   src,
					To:     dst,
					Label:  label,
					Data:   ge.Data.AsMap(),
					Loaded: true,
				}
			} else {
				e = &gdbi.Edge{
					ID:     eid,
					From:   src,
					To:     dst,
					Label:  label,
					Loaded: false,
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
func (kgdb *KVInterfaceGDB) GetVertexList(ctx context.Context, loadProp bool) <-chan *gdbi.Vertex {
	o := make(chan *gdbi.Vertex, 100)
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
				gv := &gripql.Vertex{}
				dataValue, _ := it.Value()
				proto.Unmarshal(dataValue, gv)
				keyValue := it.Key()
				_, vid := VertexKeyParse(keyValue)
				o <- &gdbi.Vertex{
					ID:     string(vid),
					Label:  gv.Label,
					Data:   gv.Data.AsMap(),
					Loaded: true,
				}
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
