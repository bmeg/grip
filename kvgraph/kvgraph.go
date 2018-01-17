package kvgraph

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
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

func (self *KVGraph) AddGraph(graph string) error {
	return self.kv.Set(GraphKey(graph), []byte{})
}

func (self *KVGraph) DeleteGraph(graph string) error {
	eprefix := EdgeListPrefix(graph)
	self.kv.DeletePrefix(eprefix)

	vprefix := VertexListPrefix(graph)
	self.kv.DeletePrefix(vprefix)

	sprefix := SrcEdgeListPrefix(graph)
	self.kv.DeletePrefix(sprefix)

	dprefix := DstEdgeListPrefix(graph)
	self.kv.DeletePrefix(dprefix)

	graphKey := GraphKey(graph)
	return self.kv.Delete(graphKey)
}

func (self *KVGraph) Graph(graph string) gdbi.DBI {
	return &KVInterfaceGDB{kv: self.kv, graph: graph}
}

func (self *KVGraph) Query(graph string) gdbi.QueryInterface {
	return self.Graph(graph).Query()
}

func (self *KVGraph) Close() {
	self.kv.Close()
}

func (self *KVGraph) GetGraphs() []string {
	out := make([]string, 0, 100)
	g_prefix := GraphPrefix()
	self.kv.View(func(it KVIterator) error {
		for it.Seek(g_prefix); it.Valid() && bytes.HasPrefix(it.Key(), g_prefix); it.Next() {
			out = append(out, GraphKeyParse(it.Key()))
		}
		return nil
	})
	return out
}

func (self *KVInterfaceGDB) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self)
}

func (self *KVInterfaceGDB) SetVertex(vertex aql.Vertex) error {
	d, _ := proto.Marshal(&vertex)
	k := VertexKey(self.graph, vertex.Gid)
	return self.kv.Set(k, d)
}

func (self *KVInterfaceGDB) SetEdge(edge aql.Edge) error {
	if edge.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; self.kv.HasKey(EdgeKeyPrefix(self.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		edge.Gid = eid
	}
	eid := edge.Gid
	data, _ := proto.Marshal(&edge)

	src := edge.From
	dst := edge.To
	ekey := EdgeKey(self.graph, eid, src, dst, edge.Label, EDGE_SINGLE)
	skey := SrcEdgeKey(self.graph, src, dst, eid, edge.Label, EDGE_SINGLE)
	dkey := DstEdgeKey(self.graph, src, dst, eid, edge.Label, EDGE_SINGLE)

	var err error
	err = self.kv.Set(ekey, data)
	if err != nil {
		return err
	}
	err = self.kv.Set(skey, []byte{})
	if err != nil {
		return err
	}
	err = self.kv.Set(dkey, []byte{})
	if err != nil {
		return err
	}
	return nil
}

func (self *KVInterfaceGDB) SetBundle(bundle aql.Bundle) error {
	if bundle.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; self.kv.HasKey(EdgeKeyPrefix(self.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		bundle.Gid = eid
	}
	eid := bundle.Gid
	data, _ := proto.Marshal(&bundle)

	src := bundle.From
	dst := ""
	ekey := EdgeKey(self.graph, eid, src, dst, bundle.Label, EDGE_BUNDLE)
	skey := SrcEdgeKey(self.graph, src, dst, eid, bundle.Label, EDGE_BUNDLE)

	if err := self.kv.Set(ekey, data); err != nil {
		return err
	}
	if err := self.kv.Set(skey, []byte{}); err != nil {
		return err
	}
	return nil
}

func (self *KVInterfaceGDB) DelEdge(eid string) error {
	ekey_prefix := EdgeKeyPrefix(self.graph, eid)
	var ekey []byte = nil
	self.kv.View(func(it KVIterator) error {
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), ekey_prefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did, _, _ := EdgeKeyParse(ekey)

	skey := SrcEdgeKeyPrefix(self.graph, sid, did, eid)
	dkey := DstEdgeKeyPrefix(self.graph, sid, did, eid)

	if err := self.kv.Delete(ekey); err != nil {
		return err
	}
	if err := self.kv.Delete(skey); err != nil {
		return err
	}
	if err := self.kv.Delete(dkey); err != nil {
		return err
	}
	return nil
}

func (self *KVInterfaceGDB) DelBundle(eid string) error {
	ekey_prefix := EdgeKeyPrefix(self.graph, eid)
	var ekey []byte = nil
	self.kv.View(func(it KVIterator) error {
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), ekey_prefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})
	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, _, _, _ := EdgeKeyParse(ekey)
	skey := SrcEdgeKeyPrefix(self.graph, sid, "", eid)
	if err := self.kv.Delete(ekey); err != nil {
		return err
	}
	if err := self.kv.Delete(skey); err != nil {
		return err
	}
	return nil
}

func (self *KVInterfaceGDB) DelVertex(id string) error {
	vid := VertexKey(self.graph, id)
	skey_prefix := SrcEdgePrefix(self.graph, id)
	dkey_prefix := DstEdgePrefix(self.graph, id)

	del_keys := make([][]byte, 0, 1000)

	self.kv.View(func(it KVIterator) error {
		for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), skey_prefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			_, sid, did, eid, label, etype := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(self.graph, eid, sid, did, label, etype)
			del_keys = append(del_keys, skey, ekey)
		}
		for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), dkey_prefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			_, sid, did, eid, label, etype := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(self.graph, eid, sid, did, label, etype)
			del_keys = append(del_keys, ekey)
		}
		return nil
	})

	return self.kv.Update(func(tx KVTransaction) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range del_keys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (self *KVInterfaceGDB) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		self.kv.View(func(it KVIterator) error {
			e_prefix := EdgeListPrefix(self.graph)
			for it.Seek(e_prefix); it.Valid() && bytes.HasPrefix(it.Key(), e_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, eid, sid, did, label, etype := EdgeKeyParse(key_value)
				if etype == EDGE_SINGLE {
					if loadProp {
						edge_data, _ := it.Value()
						e := aql.Edge{}
						proto.Unmarshal(edge_data, &e)
						o <- e
					} else {
						e := aql.Edge{Gid: string(eid), Label: label, From: sid, To: did}
						o <- e
					}
				} else {
					bundle := aql.Bundle{}
					edge_data, _ := it.Value()
					proto.Unmarshal(edge_data, &bundle)
					for k, v := range bundle.Bundle {
						e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
						o <- e
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (self *KVInterfaceGDB) GetInEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		dkey_prefix := DstEdgePrefix(self.graph, id)
		self.kv.View(func(it KVIterator) error {
			for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), dkey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, src, dst, eid, label, etype := DstEdgeKeyParse(key_value)
				e := aql.Edge{}
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if loadProp {
						ekey := EdgeKey(self.graph, eid, src, dst, label, etype)
						data_value, err := it.Get(ekey)
						if err == nil {
							proto.Unmarshal(data_value, &e)
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

func (self *KVInterfaceGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		//log.Printf("GetOutList")
		skey_prefix := SrcEdgePrefix(self.graph, id)
		self.kv.View(func(it KVIterator) error {
			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, src, dst, eid, label, edge_type := SrcEdgeKeyParse(key_value)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if edge_type == EDGE_SINGLE {
						e := aql.Edge{}
						if loadProp {
							ekey := EdgeKey(self.graph, eid, src, dst, label, edge_type)
							data_value, err := it.Get(ekey)
							if err == nil {
								proto.Unmarshal(data_value, &e)
							}
						} else {
							e.Gid = string(eid)
							e.From = string(src)
							e.To = dst
							e.Label = label
						}
						o <- e
					} else if edge_type == EDGE_BUNDLE {
						bundle := aql.Bundle{}
						ekey := EdgeKey(self.graph, eid, src, "", label, edge_type)
						data_value, err := it.Get(ekey)
						if err == nil {
							proto.Unmarshal(data_value, &bundle)
							for k, v := range bundle.Bundle {
								e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
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

func (self *KVInterfaceGDB) GetOutBundleList(ctx context.Context, id string, load bool, edgeLabels []string) chan aql.Bundle {
	o := make(chan aql.Bundle, 100)
	go func() {
		defer close(o)
		self.kv.View(func(it KVIterator) error {
			skey_prefix := SrcEdgePrefix(self.graph, id)
			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, src, _, eid, label, etype := SrcEdgeKeyParse(key_value)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if etype == EDGE_BUNDLE {
						bundle := aql.Bundle{}
						ekey := EdgeKey(self.graph, eid, src, "", label, etype)
						data_value, err := it.Get(ekey)
						if err == nil {
							proto.Unmarshal(data_value, &bundle)
							o <- bundle
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (self *KVInterfaceGDB) GetInList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)

		self.kv.View(func(it KVIterator) error {
			dkey_prefix := DstEdgePrefix(self.graph, id)
			for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), dkey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, src, _, _, label, _ := DstEdgeKeyParse(key_value)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := VertexKey(self.graph, src)
					data_value, err := it.Get(vkey)
					if err == nil {
						v := aql.Vertex{}
						proto.Unmarshal(data_value, &v)
						o <- v
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (self *KVInterfaceGDB) GetOutList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	vertex_chan := make(chan []byte, 100)
	go func() {
		defer close(vertex_chan)
		self.kv.View(func(it KVIterator) error {
			skey_prefix := SrcEdgePrefix(self.graph, id)
			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Key()
				_, src, dst, eid, label, etype := SrcEdgeKeyParse(key_value)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := VertexKey(self.graph, dst)
					if etype == EDGE_SINGLE {
						vertex_chan <- vkey
					} else if etype == EDGE_BUNDLE {
						bkey := EdgeKey(self.graph, eid, src, "", label, etype)
						bundle_value, err := it.Get(bkey)
						if err == nil {
							bundle := aql.Bundle{}
							proto.Unmarshal(bundle_value, &bundle)
							for k, _ := range bundle.Bundle {
								vertex_chan <- VertexKey(self.graph, k)
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
		self.kv.View(func(it KVIterator) error {
			for vkey := range vertex_chan {
				data_value, err := it.Get(vkey)
				if err == nil {
					v := aql.Vertex{}
					proto.Unmarshal(data_value, &v)
					o <- v
				}
			}
			return nil
		})
	}()
	return o
}

func (self *KVInterfaceGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := VertexKey(self.graph, id)
	v := aql.Vertex{}
	self.kv.View(func(it KVIterator) error {
		data_value, err := it.Get(vkey)
		if err != nil {
			return nil
		}
		if loadProp {
			proto.Unmarshal(data_value, &v)
		} else {
			v.Gid = id
		}
		return nil
	})
	return &v
}

func (self *KVInterfaceGDB) GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex {

	data := make(chan []byte, 100)
	go func() {
		defer close(data)
		self.kv.View(func(it KVIterator) error {
			for id := range ids {
				vkey := VertexKey(self.graph, id)
				data_value, err := it.Get(vkey)
				if err == nil {
					data <- data_value
				} else {
					data <- nil
				}
			}
			return nil
		})
	}()

	out := make(chan *aql.Vertex, 100)
	go func() {
		defer close(out)
		for d := range data {
			if d != nil {
				v := aql.Vertex{}
				proto.Unmarshal(d, &v)
				out <- &v
			} else {
				out <- nil
			}
		}
	}()

	return out
}

func (self *KVInterfaceGDB) GetEdge(id string, loadProp bool) *aql.Edge {
	ekey_prefix := EdgeKeyPrefix(self.graph, id)

	var e *aql.Edge = nil
	self.kv.View(func(it KVIterator) error {
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), ekey_prefix); it.Next() {
			_, eid, src, dst, label, _ := EdgeKeyParse(it.Key())
			if loadProp {
				e := &aql.Edge{}
				d, _ := it.Value()
				proto.Unmarshal(d, e)
			} else {
				e := &aql.Edge{}
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

func (self *KVInterfaceGDB) GetBundle(id string, load bool) *aql.Bundle {
	ekey_prefix := EdgeKeyPrefix(self.graph, id)

	var e *aql.Bundle = nil
	self.kv.View(func(it KVIterator) error {
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Key(), ekey_prefix); it.Next() {
			e := &aql.Bundle{}
			d, _ := it.Value()
			proto.Unmarshal(d, e)
		}
		return nil
	})
	return e
}

func (self *KVInterfaceGDB) GetVertexList(ctx context.Context, loadProp bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		self.kv.View(func(it KVIterator) error {
			v_prefix := VertexListPrefix(self.graph)

			for it.Seek(v_prefix); it.Valid() && bytes.HasPrefix(it.Key(), v_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				v := aql.Vertex{}
				if loadProp {
					data_value, _ := it.Value()
					proto.Unmarshal(data_value, &v)
				} else {
					key_value := it.Key()
					_, vid := VertexKeyParse(key_value)
					v.Gid = string(vid)
				}
				o <- v
			}
			return nil
		})
	}()
	return o
}
