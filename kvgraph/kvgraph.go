package kvgraph

import (
	//"log"
	"bytes"
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvi"
	proto "github.com/golang/protobuf/proto"
	"math/rand"
)

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

// AddGraph creates a new graph named `graph`
func (kgraph *KVGraph) AddGraph(graph string) error {
	kgraph.ts.Touch(graph)
	kgraph.setupGraphIndex(graph)
	return kgraph.kv.Set(GraphKey(graph), []byte{})
}

// DeleteGraph deletes `graph`
func (kgraph *KVGraph) DeleteGraph(graph string) error {
	kgraph.ts.Touch(graph)
	eprefix := EdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(eprefix)

	vprefix := VertexListPrefix(graph)
	kgraph.kv.DeletePrefix(vprefix)

	sprefix := SrcEdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(sprefix)

	dprefix := DstEdgeListPrefix(graph)
	kgraph.kv.DeletePrefix(dprefix)

	graphKey := GraphKey(graph)

	kgraph.deleteGraphIndex(graph)

	return kgraph.kv.Delete(graphKey)
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *KVGraph) Graph(graph string) gdbi.GraphInterface {
	return &KVInterfaceGDB{kvg: kgraph, graph: graph}
}

// Close the connection
func (kgraph *KVGraph) Close() {
	kgraph.kv.Close()
}

// GetGraphs lists the graphs managed by this driver
func (kgraph *KVGraph) GetGraphs() []string {
	out := make([]string, 0, 100)
	gPrefix := GraphPrefix()
	kgraph.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(gPrefix); it.Valid() && bytes.HasPrefix(it.Key(), gPrefix); it.Next() {
			out = append(out, GraphKeyParse(it.Key()))
		}
		return nil
	})
	return out
}

// GetTimestamp returns the update timestamp
func (kgdb *KVInterfaceGDB) GetTimestamp() string {
	return kgdb.kvg.ts.Get(kgdb.graph)
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (kgdb *KVInterfaceGDB) AddVertex(vertexArray []*aql.Vertex) error {
	kgdb.kvg.kv.Update(func(tx kvi.KVTransaction) error {
		for _, vertex := range vertexArray {
			d, _ := proto.Marshal(vertex)
			k := VertexKey(kgdb.graph, vertex.Gid)
			err := tx.Set(k, d)
			if err != nil {
				return err
			}
			//vertexIdxStruct(vertex)
			doc := vertexIdxStruct(vertex)
			kgdb.kvg.idx.AddDocTx(tx, vertex.Gid, map[string]interface{}{kgdb.graph: doc})
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return nil
	})
	/*
		for _, vertex := range vertexArray {
			doc := vertexIdxStruct(vertex)
			//log.Printf("Indexing: %s", doc)
			kgdb.kvg.idx.AddDocPrefix(vertex.Gid, doc, kgdb.graph)
		}
	*/
	return nil
}

func randomEdgeKeyAssignment(graph string, tx kvi.KVTransaction) string {
	eid := fmt.Sprintf("%d", rand.Uint64())
	for ; tx.HasKey(EdgeKeyPrefix(graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
	}
	return eid
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (kgdb *KVInterfaceGDB) AddEdge(edgeArray []*aql.Edge) error {
	kgdb.kvg.kv.Update(func(tx kvi.KVTransaction) error {
		for _, edge := range edgeArray {
			if edge.Gid == "" {
				edge.Gid = randomEdgeKeyAssignment(kgdb.graph, tx)
			}

			eid := edge.Gid
			var err error
			var data []byte

			data, err = proto.Marshal(edge)
			if err != nil {
				return err
			}

			src := edge.From
			dst := edge.To
			ekey := EdgeKey(kgdb.graph, eid, src, dst, edge.Label, edgeSingle)
			skey := SrcEdgeKey(kgdb.graph, src, dst, eid, edge.Label, edgeSingle)
			dkey := DstEdgeKey(kgdb.graph, src, dst, eid, edge.Label, edgeSingle)

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
		}
		kgdb.kvg.ts.Touch(kgdb.graph)
		return nil
	})
	return nil
}

// AddBundle adds a bundle to the graph
func (kgdb *KVInterfaceGDB) AddBundle(bundle *aql.Bundle) error {
	if bundle.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; kgdb.kvg.kv.HasKey(EdgeKeyPrefix(kgdb.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		bundle.Gid = eid
	}
	eid := bundle.Gid
	data, _ := proto.Marshal(bundle)

	src := bundle.From
	dst := ""
	ekey := EdgeKey(kgdb.graph, eid, src, dst, bundle.Label, edgeBundle)
	skey := SrcEdgeKey(kgdb.graph, src, dst, eid, bundle.Label, edgeBundle)

	if err := kgdb.kvg.kv.Set(ekey, data); err != nil {
		return err
	}
	if err := kgdb.kvg.kv.Set(skey, []byte{}); err != nil {
		return err
	}
	kgdb.kvg.ts.Touch(kgdb.graph)
	return nil
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

// DelBundle removes a bundle of edges given an id
func (kgdb *KVInterfaceGDB) DelBundle(eid string) error {
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

	_, _, sid, _, _, _ := EdgeKeyParse(ekey)
	skey := SrcEdgeKeyPrefix(kgdb.graph, sid, "", eid)
	if err := kgdb.kvg.kv.Delete(ekey); err != nil {
		return err
	}
	if err := kgdb.kvg.kv.Delete(skey); err != nil {
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
func (kgdb *KVInterfaceGDB) GetEdgeList(ctx context.Context, loadProp bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
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
						e := &aql.Edge{}
						proto.Unmarshal(edgeData, e)
						o <- e
					} else {
						e := &aql.Edge{Gid: string(eid), Label: label, From: sid, To: did}
						o <- e
					}
				} else {
					bundle := &aql.Bundle{}
					edgeData, _ := it.Value()
					proto.Unmarshal(edgeData, bundle)
					for k, v := range bundle.Bundle {
						e := &aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
						o <- e
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (kgdb *KVInterfaceGDB) GetInEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
		dkeyPrefix := DstEdgePrefix(kgdb.graph, id)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, src, dst, eid, label, etype := DstEdgeKeyParse(keyValue)
				e := &aql.Edge{}
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if loadProp {
						ekey := EdgeKey(kgdb.graph, eid, src, dst, label, etype)
						dataValue, err := it.Get(ekey)
						if err == nil {
							proto.Unmarshal(dataValue, e)
						}
					} else {
						e.Gid = string(eid)
						e.From = string(src)
						e.Label = label
						e.To = dst
					}
					o <- e
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (kgdb *KVInterfaceGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
		//log.Printf("GetOutList")
		skeyPrefix := SrcEdgePrefix(kgdb.graph, id)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, src, dst, eid, label, edgeType := SrcEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if edgeType == edgeSingle {
						e := &aql.Edge{}
						if loadProp {
							ekey := EdgeKey(kgdb.graph, eid, src, dst, label, edgeType)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, e)
							}
						} else {
							e.Gid = string(eid)
							e.From = string(src)
							e.To = dst
							e.Label = label
						}
						o <- e
					} else if edgeType == edgeBundle {
						bundle := &aql.Bundle{}
						ekey := EdgeKey(kgdb.graph, eid, src, "", label, edgeType)
						dataValue, err := it.Get(ekey)
						if err == nil {
							proto.Unmarshal(dataValue, bundle)
							for k, v := range bundle.Bundle {
								e := &aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
								o <- e
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

// GetOutBundleChannel given vertex `key` find all outgoing bundles,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
// load is ignored
func (kgdb *KVInterfaceGDB) GetOutBundleChannel(ids chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for id := range ids {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, id.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, src, _, eid, label, etype := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if etype == edgeBundle {
							bundle := &aql.Bundle{}
							ekey := EdgeKey(kgdb.graph, eid, src, "", label, etype)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, bundle)
								id.Bundle = bundle
								o <- id
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

// GetInList given vertex/edge `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (kgdb *KVInterfaceGDB) GetInList(ctx context.Context, id string, loadProp bool, edgeLabels []string) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
	go func() {
		defer close(o)

		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			dkeyPrefix := DstEdgePrefix(kgdb.graph, id)
			for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, src, _, _, label, _ := DstEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := VertexKey(kgdb.graph, src)
					dataValue, err := it.Get(vkey)
					if err == nil {
						v := &aql.Vertex{}
						proto.Unmarshal(dataValue, v)
						o <- v
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (kgdb *KVInterfaceGDB) GetOutList(ctx context.Context, id string, loadProp bool, edgeLabels []string) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
	vertexChan := make(chan []byte, 100)
	go func() {
		defer close(vertexChan)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			skeyPrefix := SrcEdgePrefix(kgdb.graph, id)
			for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, src, dst, eid, label, etype := SrcEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := VertexKey(kgdb.graph, dst)
					if etype == edgeSingle {
						vertexChan <- vkey
					} else if etype == edgeBundle {
						bkey := EdgeKey(kgdb.graph, eid, src, "", label, etype)
						bundleValue, err := it.Get(bkey)
						if err == nil {
							bundle := &aql.Bundle{}
							proto.Unmarshal(bundleValue, bundle)
							for k := range bundle.Bundle {
								vertexChan <- VertexKey(kgdb.graph, k)
							}
						}
					}
				}
			}
			return nil
		})
	}()

	go func() {
		defer close(o)
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for vkey := range vertexChan {
				dataValue, err := it.Get(vkey)
				if err == nil {
					v := &aql.Vertex{}
					proto.Unmarshal(dataValue, v)
					o <- v
				}
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (kgdb *KVInterfaceGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := VertexKey(kgdb.graph, id)
	v := &aql.Vertex{}
	kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		dataValue, err := it.Get(vkey)
		if err != nil {
			return nil
		}
		if loadProp {
			proto.Unmarshal(dataValue, v)
		} else {
			v.Gid = id
		}
		return nil
	})
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
			v := aql.Vertex{}
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
					_, src, dst, eid, label, etype := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						vkey := VertexKey(kgdb.graph, dst)
						if etype == edgeSingle {
							vertexChan <- elementData{
								data: vkey,
								req:  req,
							}
						} else if etype == edgeBundle {
							bkey := EdgeKey(kgdb.graph, eid, src, "", label, etype)
							bundleValue, err := it.Get(bkey)
							if err == nil {
								bundle := aql.Bundle{}
								proto.Unmarshal(bundleValue, &bundle)
								for k := range bundle.Bundle {
									vertexChan <- elementData{
										data: VertexKey(kgdb.graph, k),
										req:  req,
									}
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
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range vertexChan {
				dataValue, err := it.Get(req.data)
				if err == nil {
					v := aql.Vertex{}
					proto.Unmarshal(dataValue, &v)
					req.req.Vertex = &v
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
							v := aql.Vertex{}
							proto.Unmarshal(dataValue, &v)
							req.Vertex = &v
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
		//log.Printf("GetOutList")
		kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				skeyPrefix := SrcEdgePrefix(kgdb.graph, req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					_, src, dst, eid, label, edgeType := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || contains(edgeLabels, label) {
						if edgeType == edgeSingle {
							e := aql.Edge{}
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
						} else if edgeType == edgeBundle {
							bundle := aql.Bundle{}
							ekey := EdgeKey(kgdb.graph, eid, src, "", label, edgeType)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, &bundle)
								for k, v := range bundle.Bundle {
									e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
									req.Edge = &e
									o <- req
								}
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
							e := aql.Edge{}
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
						} else if edgeType == edgeBundle {
							bundle := aql.Bundle{}
							ekey := EdgeKey(kgdb.graph, eid, src, "", label, edgeType)
							dataValue, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(dataValue, &bundle)
								for k, v := range bundle.Bundle {
									e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
									req.Edge = &e
									o <- req
								}
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
func (kgdb *KVInterfaceGDB) GetEdge(id string, loadProp bool) *aql.Edge {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, id)

	e := &aql.Edge{}
	kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst, label, _ := EdgeKeyParse(it.Key())
			if loadProp {
				e = &aql.Edge{}
				d, _ := it.Value()
				proto.Unmarshal(d, e)
			} else {
				e = &aql.Edge{}
				e.Gid = eid
				e.From = src
				e.To = dst
				e.Label = label
			}
		}
		return nil
	})
	return e
}

// GetBundle loads bundle of edges, given an id
// loadProp is ignored
func (kgdb *KVInterfaceGDB) GetBundle(id string, load bool) *aql.Bundle {
	ekeyPrefix := EdgeKeyPrefix(kgdb.graph, id)
	e := &aql.Bundle{}
	kgdb.kvg.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			d, _ := it.Value()
			proto.Unmarshal(d, e)
		}
		return nil
	})
	return e
}

// GetVertexList produces a channel of all edges in the graph
func (kgdb *KVInterfaceGDB) GetVertexList(ctx context.Context, loadProp bool) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
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
				v := &aql.Vertex{}
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
