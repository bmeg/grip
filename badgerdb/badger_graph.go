package badgerdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"os"
)

type BadgerArachne struct {
	kv *badger.DB
}

type BadgerGDB struct {
	kv    *badger.DB
	graph string
}

func HasKey(kv *badger.DB, key []byte) bool {
	out := false
	kv.View(func(tx *badger.Txn) error {
		_, err := tx.Get(key)
		if err == nil {
			out = true
		}
		return nil
	})
	return out
}

var GRAPH_PREFIX []byte = []byte("g")
var VERTEX_PREFIX []byte = []byte("v")
var EDGE_PREFIX []byte = []byte("e")
var SEDGE_PREFIX []byte = []byte("s")
var DEDGE_PREFIX []byte = []byte("d")

var EDGE_SINGLE byte = 0x00
var EDGE_BUNDLE byte = 0x01

func GraphKey(graph string) []byte {
	return bytes.Join([][]byte{GRAPH_PREFIX, []byte(graph)}, []byte{0})
}

func GraphPrefix() []byte {
	return GRAPH_PREFIX
}

func GraphKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	graph := string(tmp[1])
	return graph
}

func EdgeKey(graph, id, src, dst string) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph), []byte(id), []byte(src), []byte(dst)}, []byte{0})
}

func EdgeKeyPrefix(graph, id string) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func EdgeKeyParse(key []byte) (string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	eid := tmp[2]
	sid := tmp[3]
	did := tmp[4]
	return string(graph), string(eid), string(sid), string(did)
}

func VertexListPrefix(graph string) []byte {
	return bytes.Join([][]byte{VERTEX_PREFIX, []byte(graph)}, []byte{0})
}

func EdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph)}, []byte{0})
}

func SrcEdgeKey(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
}

func SrcEdgeKeyParse(key []byte) (string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	src := tmp[2]
	dst := tmp[3]
	eid := tmp[4]
	return string(graph), string(src), string(dst), string(eid)
}

func DstEdgeKeyParse(key []byte) (string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	dst := tmp[2]
	src := tmp[3]
	eid := tmp[4]
	return string(graph), string(src), string(dst), string(eid)
}

func SrcEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph)}, []byte{0})
}

func DstEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph)}, []byte{0})
}

func SrcEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func DstEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func DstEdgeKey(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph), []byte(dst), []byte(src), []byte(eid)}, []byte{0})
}

func VertexKey(graph, id string) []byte {
	return bytes.Join([][]byte{VERTEX_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func VertexKeyParse(key []byte) (string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	vid := tmp[2]
	return string(graph), string(vid)
}

func NewBadgerArachne(path string) gdbi.ArachneInterface {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	opts := badger.Options{}
	opts = badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	kv, err := badger.Open(opts)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	return &BadgerArachne{kv: kv}
}

func (self *BadgerArachne) AddGraph(graph string) error {
	self.kv.Update(func(tx *badger.Txn) error {
		tx.Set(GraphKey(graph), []byte{}, 0x00)
		return nil
	})
	return nil
}

func bytes_copy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func (self *BadgerArachne) prefixDelete(prefix []byte) {
	DELETE_BLOCK_SIZE := 10000

	for found := true; found; {
		found = false
		wb := make([][]byte, 0, DELETE_BLOCK_SIZE)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		self.kv.Update(func(tx *badger.Txn) error {
			it := tx.NewIterator(opts)
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), prefix) && len(wb) < DELETE_BLOCK_SIZE-1; it.Next() {
				wb = append(wb, bytes_copy(it.Item().Key()))
			}
			it.Close()
			for _, i := range wb {
				tx.Delete(i)
				found = true
			}
			return nil
		})
	}
}

func (self *BadgerArachne) DeleteGraph(graph string) error {
	eprefix := EdgeListPrefix(graph)
	self.prefixDelete(eprefix)

	vprefix := VertexListPrefix(graph)
	self.prefixDelete(vprefix)

	sprefix := SrcEdgeListPrefix(graph)
	self.prefixDelete(sprefix)

	dprefix := DstEdgeListPrefix(graph)
	self.prefixDelete(dprefix)

	graphKey := GraphKey(graph)
	self.kv.Update(func(tx *badger.Txn) error {
		tx.Delete(graphKey)
		return nil
	})

	return nil
}

func (self *BadgerArachne) Graph(graph string) gdbi.DBI {
	return &BadgerGDB{kv: self.kv, graph: graph}
}

func (self *BadgerArachne) Query(graph string) gdbi.QueryInterface {
	return self.Graph(graph).Query()
}

func (self *BadgerArachne) Close() {
	self.kv.Close()
}

func (self *BadgerArachne) GetGraphs() []string {
	out := make([]string, 0, 100)
	g_prefix := GraphPrefix()
	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(g_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), g_prefix); it.Next() {
			out = append(out, GraphKeyParse(it.Item().Key()))
		}
		return nil
	})
	return out
}

func (self *BadgerGDB) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self)
}

func (self *BadgerGDB) SetVertex(vertex aql.Vertex) error {
	d, _ := proto.Marshal(&vertex)
	k := VertexKey(self.graph, vertex.Gid)
	err := self.kv.Update(func(tx *badger.Txn) error {
		return tx.Set(k, d, 0x00)
	})
	return err
}

func (self *BadgerGDB) SetEdge(edge aql.Edge) error {
	if edge.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; HasKey(self.kv, EdgeKeyPrefix(self.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		edge.Gid = eid
	}
	eid := edge.Gid
	data, _ := proto.Marshal(&edge)

	src := edge.From
	dst := edge.To
	ekey := EdgeKey(self.graph, eid, src, dst)
	skey := SrcEdgeKey(self.graph, src, dst, eid)
	dkey := DstEdgeKey(self.graph, src, dst, eid)

	return self.kv.Update(func(tx *badger.Txn) error {
		err := tx.Set(ekey, data, EDGE_SINGLE)
		if err != nil {
			return err
		}
		err = tx.Set(skey, []byte{}, EDGE_SINGLE)
		if err != nil {
			return err
		}
		err = tx.Set(dkey, []byte{}, EDGE_SINGLE)
		if err != nil {
			return err
		}
		return nil
	})
}

func (self *BadgerGDB) SetBundle(bundle aql.Bundle) error {
	if bundle.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; HasKey(self.kv, EdgeKeyPrefix(self.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		bundle.Gid = eid
	}
	eid := bundle.Gid
	data, _ := proto.Marshal(&bundle)

	src := bundle.From
	dst := ""
	ekey := EdgeKey(self.graph, eid, src, dst)
	skey := SrcEdgeKey(self.graph, src, dst, eid)

	return self.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Set(ekey, data, EDGE_BUNDLE); err != nil {
			return err
		}
		if err := tx.Set(skey, []byte{}, EDGE_BUNDLE); err != nil {
			return err
		}
		return nil
	})
}

func (self *BadgerGDB) DelEdge(eid string) error {
	ekey_prefix := EdgeKeyPrefix(self.graph, eid)
	var ekey []byte = nil
	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
			ekey = it.Item().Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did := EdgeKeyParse(ekey)

	skey := SrcEdgeKey(self.graph, sid, did, eid)
	dkey := DstEdgeKey(self.graph, sid, did, eid)

	return self.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Delete(ekey); err != nil {
			return err
		}
		if err := tx.Delete(skey); err != nil {
			return err
		}
		if err := tx.Delete(dkey); err != nil {
			return err
		}
		return nil
	})
}

func (self *BadgerGDB) DelBundle(eid string) error {
	ekey_prefix := EdgeKeyPrefix(self.graph, eid)
	var ekey []byte = nil
	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
			ekey = it.Item().Key()
		}
		return nil
	})
	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, _ := EdgeKeyParse(ekey)

	skey := SrcEdgeKey(self.graph, sid, "", eid)

	return self.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Delete(ekey); err != nil {
			return err
		}
		if err := tx.Delete(skey); err != nil {
			return err
		}
		return nil
	})
}

func (self *BadgerGDB) DelVertex(id string) error {
	vid := VertexKey(self.graph, id)
	skey_prefix := SrcEdgePrefix(self.graph, id)
	dkey_prefix := DstEdgePrefix(self.graph, id)

	del_keys := make([][]byte, 0, 1000)

	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
			skey := it.Item().Key()
			// get edge ID from key
			_, sid, did, eid := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(self.graph, eid, sid, did)
			del_keys = append(del_keys, skey, ekey)
		}
		for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dkey_prefix); it.Next() {
			dkey := it.Item().Key()
			// get edge ID from key
			_, sid, did, eid := SrcEdgeKeyParse(dkey)
			ekey := EdgeKey(self.graph, eid, sid, did)
			del_keys = append(del_keys, ekey)
		}
		return nil
	})

	return self.kv.Update(func(tx *badger.Txn) error {
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

func (self *BadgerGDB) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		self.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			e_prefix := EdgeListPrefix(self.graph)
			for it.Seek(e_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), e_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, eid, sid, did := EdgeKeyParse(key_value)
				if it.Item().UserMeta() == EDGE_SINGLE {
					if loadProp {
						edge_data, _ := it.Item().Value()
						e := aql.Edge{}
						proto.Unmarshal(edge_data, &e)
						o <- e
					} else {
						e := aql.Edge{Gid: string(eid), From: sid, To: did}
						o <- e
					}
				} else {
					bundle := aql.Bundle{}
					edge_data, _ := it.Item().Value()
					proto.Unmarshal(edge_data, &bundle)
					for k, v := range bundle.Bundle {
						e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
						o <- e
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (self *BadgerGDB) GetInEdgeList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		dkey_prefix := DstEdgePrefix(self.graph, id)
		self.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dkey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, src, dst, eid := DstEdgeKeyParse(key_value)

				e := aql.Edge{}
				if loadProp {
					ekey := EdgeKey(self.graph, eid, src, dst)
					data_value, err := tx.Get(ekey)
					if err == nil {
						d, _ := data_value.Value()
						proto.Unmarshal(d, &e)
					}
				} else {
					e.Gid = string(eid)
					e.From = string(src)
					e.To = dst
				}

				send := false
				if filter != nil {
					if filter(e) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- e
				}
			}
			return nil
		})
	}()
	return o
}

func (self *BadgerGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		//log.Printf("GetOutList")
		skey_prefix := SrcEdgePrefix(self.graph, id)
		self.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, src, dst, eid := SrcEdgeKeyParse(key_value)

				//log.Printf("Found %s %s %s", src, dst, eid)
				if it.Item().UserMeta() == EDGE_SINGLE {
					e := aql.Edge{}
					if loadProp {
						ekey := EdgeKey(self.graph, eid, src, dst)
						data_value, err := tx.Get(ekey)
						if err == nil {
							d, _ := data_value.Value()
							proto.Unmarshal(d, &e)
						}
					} else {
						e.Gid = string(eid)
						e.From = string(src)
						e.To = dst
					}
					if filter != nil {
						if filter(e) {
							o <- e
						}
					} else {
						o <- e
					}
				} else if it.Item().UserMeta() == EDGE_BUNDLE {
					bundle := aql.Bundle{}
					ekey := EdgeKey(self.graph, eid, src, "")
					data_value, err := tx.Get(ekey)
					if err == nil {
						d, _ := data_value.Value()
						proto.Unmarshal(d, &bundle)
						for k, v := range bundle.Bundle {
							e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
							if filter != nil {
								if filter(e) {
									o <- e
								}
							} else {
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

func (self *BadgerGDB) GetOutBundleList(ctx context.Context, id string, load bool, filter gdbi.BundleFilter) chan aql.Bundle {
	o := make(chan aql.Bundle, 100)
	go func() {
		defer close(o)
		//log.Printf("GetOutList")
		self.kv.View(func(tx *badger.Txn) error {
			skey_prefix := SrcEdgePrefix(self.graph, id)
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, src, _, eid := SrcEdgeKeyParse(key_value)

				//log.Printf("Found %s %s %s", src, dst, eid)
				if it.Item().UserMeta() == EDGE_BUNDLE {
					bundle := aql.Bundle{}
					ekey := EdgeKey(self.graph, eid, src, "")
					data_value, err := tx.Get(ekey)
					if err == nil {
						d, _ := data_value.Value()
						proto.Unmarshal(d, &bundle)
						if filter != nil {
							if filter(bundle) {
								o <- bundle
							}
						} else {
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

func (self *BadgerGDB) GetInList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)

		self.kv.View(func(tx *badger.Txn) error {
			dkey_prefix := DstEdgePrefix(self.graph, id)
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dkey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, src, dst, eid := DstEdgeKeyParse(key_value)

				ekey := EdgeKey(self.graph, eid, src, dst)
				vkey := VertexKey(self.graph, src)

				send := false
				if filter != nil {
					data_value, err := tx.Get(ekey)
					if err == nil {
						d, _ := data_value.Value()
						e := aql.Edge{}
						proto.Unmarshal(d, &e)
						if filter(e) {
							send = true
						}
					}
				} else {
					send = true
				}
				if send {
					data_value, err := tx.Get(vkey)
					if err == nil {
						d, _ := data_value.Value()
						v := aql.Vertex{}
						proto.Unmarshal(d, &v)
						o <- v
					}
				}
			}
			return nil
		})

	}()
	return o
}

func (self *BadgerGDB) GetOutList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)

	vertex_chan := make(chan []byte, 100)

	go func() {
		defer close(vertex_chan)
		self.kv.View(func(tx *badger.Txn) error {
			skey_prefix := SrcEdgePrefix(self.graph, id)
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				key_value := it.Item().Key()
				_, src, dst, eid := SrcEdgeKeyParse(key_value)

				vkey := VertexKey(self.graph, dst)

				if it.Item().UserMeta() == EDGE_SINGLE {
					ekey := EdgeKey(self.graph, eid, src, dst)
					if filter != nil {
						data_value, err := tx.Get(ekey)
						if err == nil {
							e := aql.Edge{}
							data, _ := data_value.Value()
							proto.Unmarshal(data, &e)
							if filter(e) {
								vertex_chan <- vkey
							}
						}
					} else {
						vertex_chan <- vkey
					}
				} else if it.Item().UserMeta() == EDGE_BUNDLE {
					bkey := EdgeKey(self.graph, eid, src, "")
					bundle_value, err := tx.Get(bkey)
					if err == nil {
						bundle := aql.Bundle{}
						data, _ := bundle_value.Value()
						proto.Unmarshal(data, &bundle)
						for k, v := range bundle.Bundle {
							e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
							if filter != nil {
								if filter(e) {
									vertex_chan <- VertexKey(self.graph, k)
								}
							} else {
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
		self.kv.View(func(tx *badger.Txn) error {
			for vkey := range vertex_chan {
				data_value, err := tx.Get(vkey)
				if err == nil {
					d, _ := data_value.Value()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					o <- v
				}
			}
			return nil
		})
	}()
	return o
}

func (self *BadgerGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := VertexKey(self.graph, id)
	v := aql.Vertex{}
	self.kv.View(func(tx *badger.Txn) error {
		data_value, err := tx.Get(vkey)
		if err != nil {
			return nil
		}
		if loadProp {
			d, _ := data_value.Value()
			proto.Unmarshal(d, &v)
		} else {
			v.Gid = id
		}
		return nil
	})
	return &v
}

func (self *BadgerGDB) GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex {

	data := make(chan []byte, 100)
	go func() {
		defer close(data)
		self.kv.View(func(tx *badger.Txn) error {
			for id := range ids {
				vkey := VertexKey(self.graph, id)
				data_value, err := tx.Get(vkey)
				if err == nil {
					d, _ := data_value.Value()
					data <- d
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

func (self *BadgerGDB) GetEdge(id string, loadProp bool) *aql.Edge {
	ekey_prefix := EdgeKeyPrefix(self.graph, id)

	var e *aql.Edge = nil
	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
			_, eid, src, dst := EdgeKeyParse(it.Item().Key())
			if loadProp {
				e := &aql.Edge{}
				d, _ := it.Item().Value()
				proto.Unmarshal(d, e)
			} else {
				e := &aql.Edge{}
				e.Gid = eid
				e.From = src
				e.To = dst
			}
		}
		return nil
	})
	return e
}

func (self *BadgerGDB) GetBundle(id string, load bool) *aql.Bundle {
	ekey_prefix := EdgeKeyPrefix(self.graph, id)

	var e *aql.Bundle = nil
	self.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
			e := &aql.Bundle{}
			d, _ := it.Item().Value()
			proto.Unmarshal(d, e)
		}
		return nil
	})
	return e
}

func (self *BadgerGDB) GetVertexList(ctx context.Context, loadProp bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		self.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			v_prefix := VertexListPrefix(self.graph)

			for it.Seek(v_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), v_prefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				v := aql.Vertex{}
				if loadProp {
					data_value, _ := it.Item().Value()
					proto.Unmarshal(data_value, &v)
				} else {
					key_value := it.Item().Key()
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
