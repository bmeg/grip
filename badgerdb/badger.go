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
	kv *badger.KV
}

type BadgerGDB struct {
	kv    *badger.KV
	graph string
}

func HasKey(kv *badger.KV, key []byte) bool {
	data_value := badger.KVItem{}
	kv.Get(key, &data_value)
	return data_value.Value() != nil
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

func NewBadgerArachne(path string) gdbi.ArachneInterface {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	opts := &badger.Options{}
	*opts = badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	kv, err := badger.NewKV(opts)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	return &BadgerArachne{kv: kv}
}

func (self *BadgerArachne) AddGraph(graph string) error {
	self.kv.Set(GraphKey(graph), []byte{}, 0x00)
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
		wb := make([]*badger.Entry, 0, DELETE_BLOCK_SIZE)
		opts := badger.DefaultIteratorOptions
		opts.FetchValues = false
		it := self.kv.NewIterator(opts)
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), prefix) && len(wb) < DELETE_BLOCK_SIZE-1; it.Next() {
			wb = badger.EntriesDelete(wb, bytes_copy(it.Item().Key()))
		}
		it.Close()
		if len(wb) > 0 {
			self.kv.BatchSet(wb)
			found = true
		}
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
	self.kv.Delete(graphKey)

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
	it := self.kv.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(g_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), g_prefix); it.Next() {
		out = append(out, GraphKeyParse(it.Item().Key()))
	}
	return out
}

func (self *BadgerGDB) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self, false)
}

func (self *BadgerGDB) SetVertex(vertex aql.Vertex) error {
	d, _ := proto.Marshal(&vertex)
	k := VertexKey(self.graph, vertex.Gid)
	err := self.kv.Set(k, d, 0x00)
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

	entries := make([]*badger.Entry, 3)
	entries[0] = &badger.Entry{Key: ekey, Value: data, UserMeta: EDGE_SINGLE}
	entries[1] = &badger.Entry{Key: skey, Value: []byte{}, UserMeta: EDGE_SINGLE}
	entries[2] = &badger.Entry{Key: dkey, Value: []byte{}, UserMeta: EDGE_SINGLE}
	self.kv.BatchSet(entries)
	for _, e := range entries {
		if e.Error != nil {
			return e.Error
		}
	}
	return nil
}

func (self *BadgerGDB) DelEdge(eid string) error {
	ekey_prefix := EdgeKeyPrefix(self.graph, eid)
	var ekey []byte = nil
	it := self.kv.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
		ekey = it.Item().Key()
	}

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did := EdgeKeyParse(ekey)

	skey := SrcEdgeKey(self.graph, sid, did, eid)
	dkey := DstEdgeKey(self.graph, sid, did, eid)

	fin := make(chan error)
	go func() {
		if err := self.kv.Delete(ekey); err != nil {
			fin <- err
			return
		}
		fin <- nil
	}()
	go func() {
		if err := self.kv.Delete(skey); err != nil {
			fin <- err
			return
		}
		fin <- nil
	}()
	go func() {
		if err := self.kv.Delete(dkey); err != nil {
			fin <- err
			return
		}
		fin <- nil
	}()
	<-fin
	<-fin
	<-fin
	return nil
}

func (self *BadgerGDB) DelVertex(id string) error {
	vid := VertexKey(self.graph, id)
	self.kv.Delete(vid)

	skey_prefix := SrcEdgePrefix(self.graph, id)
	dkey_prefix := DstEdgePrefix(self.graph, id)

	del_keys := make([][]byte, 0, 1000)

	it := self.kv.NewIterator(badger.DefaultIteratorOptions)
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

	entries := make([]*badger.Entry, 0, 100)
	for _, k := range del_keys {
		//log.Printf("Delete %s", string(bytes.Replace(k, []byte{0}, []byte{' '}, -1) ) )
		entries = badger.EntriesDelete(entries, k)
	}
	err := self.kv.BatchSet(entries)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Error != nil {
			return e.Error
		}
	}
	return nil
}

func (self *BadgerGDB) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		e_prefix := EdgeListPrefix(self.graph)
		for it.Seek(e_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), e_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			_, eid, sid, did := EdgeKeyParse(key_value)
			if loadProp {
				edge_data := it.Item().Value()
				e := aql.Edge{}
				proto.Unmarshal(edge_data, &e)
				o <- e
			} else {
				e := aql.Edge{Gid: string(eid), From: sid, To: did}
				o <- e
			}
		}
	}()
	return o
}

func (self *BadgerGDB) GetInEdgeList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)

		dkey_prefix := DstEdgePrefix(self.graph, id)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dkey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			_, src, dst, eid := DstEdgeKeyParse(key_value)

			e := aql.Edge{}
			if loadProp {
				ekey := EdgeKey(self.graph, eid, src, dst)
				data_value := badger.KVItem{}
				err := self.kv.Get(ekey, &data_value)
				if err == nil {
					d := data_value.Value()
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
	}()
	return o
}

func (self *BadgerGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		log.Printf("GetOutList")
		skey_prefix := SrcEdgePrefix(self.graph, id)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			_, src, dst, eid := SrcEdgeKeyParse(key_value)

			log.Printf("Found %s %s %s", src, dst, eid)

			e := aql.Edge{}
			if loadProp {
				ekey := EdgeKey(self.graph, eid, src, dst)
				data_value := badger.KVItem{}
				err := self.kv.Get(ekey, &data_value)
				if err == nil {
					d := data_value.Value()
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
	}()
	return o
}

func (self *BadgerGDB) GetInList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)

		dkey_prefix := DstEdgePrefix(self.graph, id)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(dkey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dkey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			_, src, dst, eid := DstEdgeKeyParse(key_value)

			ekey := EdgeKey(self.graph, eid, src, dst)
			vkey := VertexKey(self.graph, src)

			send := false
			if filter != nil {
				data_value := badger.KVItem{}
				err := self.kv.Get(ekey, &data_value)
				if err == nil {
					d := data_value.Value()
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
				data_value := badger.KVItem{}
				err := self.kv.Get(vkey, &data_value)
				if err == nil {
					d := data_value.Value()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					o <- v
				}
			}
		}
	}()
	return o
}

func (self *BadgerGDB) GetOutList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)

		skey_prefix := SrcEdgePrefix(self.graph, id)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(skey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), skey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			_, src, dst, eid := SrcEdgeKeyParse(key_value)

			vkey := VertexKey(self.graph, dst)
			ekey := EdgeKey(self.graph, eid, src, dst)
			send := false
			if filter != nil {
				data_value := badger.KVItem{}
				err := self.kv.Get(ekey, &data_value)
				if err == nil {
					e := aql.Edge{}
					proto.Unmarshal(data_value.Value(), &e)
					if filter(e) {
						send = true
					}
				}
			} else {
				send = true
			}
			if send {
				data_value := badger.KVItem{}
				err := self.kv.Get(vkey, &data_value)
				if err == nil {
					d := data_value.Value()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					o <- v
				}
			}
		}
	}()
	return o
}

func (self *BadgerGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := VertexKey(self.graph, id) //bytes.Join([][]byte{[]byte("v"), []byte(id)}, []byte{0})
	data_value := badger.KVItem{}
	err := self.kv.Get(vkey, &data_value)
	if err != nil || data_value.Value() == nil {
		return nil
	}
	v := aql.Vertex{}
	if loadProp {
		d := data_value.Value()
		proto.Unmarshal(d, &v)
	} else {
		v.Gid = id
	}
	return &v
}

func (self *BadgerGDB) GetEdge(id string, loadProp bool) *aql.Edge {
	ekey_prefix := EdgeKeyPrefix(self.graph, id)

	var e *aql.Edge = nil
	it := self.kv.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(ekey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekey_prefix); it.Next() {
		_, eid, src, dst := EdgeKeyParse(it.Item().Key())
		if loadProp {
			e := &aql.Edge{}
			d := it.Item().Value()
			proto.Unmarshal(d, e)
		} else {
			e := &aql.Edge{}
			e.Gid = eid
			e.From = src
			e.To = dst
		}
	}
	return e
}

func (self *BadgerGDB) GetVertexList(ctx context.Context, loadProp bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		v_prefix := VertexListPrefix(self.graph)

		for it.Seek(v_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), v_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := aql.Vertex{}
			if loadProp {
				data_value := it.Item().Value()
				proto.Unmarshal(data_value, &v)
			} else {
				key_value := it.Item().Key()
				tmp := bytes.Split(key_value, []byte{0})
				iid := tmp[1]
				v.Gid = string(iid)
			}
			o <- v
		}
	}()
	return o
}
