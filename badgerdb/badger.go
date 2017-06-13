package badgerdb

import (
	"os"
	"fmt"
	"bytes"
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/dgraph-io/badger/badger"
	proto "github.com/golang/protobuf/proto"
	"log"
)

type BadgerGDB struct {
	kv       *badger.KV
	sequence int64
}

func NewBadgerArachne(path string) gdbi.DBI {

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
	return &BadgerGDB{kv: kv}
}

func (self *BadgerGDB) Close() {
	self.kv.Close()
}

func (self *BadgerGDB) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self, false)
	return nil
}

func (self *BadgerGDB) SetVertex(vertex aql.Vertex) error {
	d, _ := proto.Marshal(&vertex)
	k := bytes.Join([][]byte{[]byte("v"), []byte(vertex.Gid)}, []byte{0})
	err := self.kv.Set(k, d)
	return err
}

func (self *BadgerGDB) SetEdge(edge aql.Edge) error {
	eid := fmt.Sprintf("%d", self.sequence)
	self.sequence += 1
	edge.Gid = eid
	data, _ := proto.Marshal(&edge)
	//log.Printf("SetEdge: %s %d", edge, len(data))

	src := edge.Src
	dst := edge.Dst
	ekey := bytes.Join([][]byte{[]byte("e"), []byte(eid)}, []byte{0})
	okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
	ikey := bytes.Join([][]byte{[]byte("i"), []byte(dst), []byte(src), []byte(eid)}, []byte{0})

	entries := make([]*badger.Entry, 3)
	entries[0] = &badger.Entry{Key: ekey, Value: okey}
	entries[1] = &badger.Entry{Key: okey, Value: data}
	entries[2] = &badger.Entry{Key: ikey, Value: []byte{}}
	self.kv.BatchSet(entries)
	for _, e := range entries {
		if e.Error != nil {
			return e.Error
		}
	}
	return nil
}

func (self *BadgerGDB) DelEdge(eid string) error {
	ekey := bytes.Join([][]byte{[]byte("e"), []byte(eid)}, []byte{0})
	item := badger.KVItem{}
	err := self.kv.Get(ekey, &item)
	if err != nil {
		return err
	}
	pair_value := item.Value()

	pair := bytes.Split(pair_value, []byte{0})
	src := pair[1]
	dst := pair[2]

	okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
	ikey := bytes.Join([][]byte{[]byte("i"), []byte(dst), []byte(src), []byte(eid)}, []byte{0})

	fin := make(chan error)
	go func() {
		if err := self.kv.Delete(ekey); err != nil {
			fin <- err
			return
		}
		fin <- nil
	}()
	go func() {
		if err := self.kv.Delete(okey); err != nil {
			fin <- err
			return
		}
		fin <- nil
	}()
	go func() {
		if err := self.kv.Delete(ikey); err != nil {
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
	vid := bytes.Join([][]byte{[]byte("v"), []byte(id)}, []byte{0})
	self.kv.Delete(vid)

	okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
	ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})

	del_keys := make([][]byte, 0, 1000)

	it := self.kv.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(okey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), okey_prefix); it.Next() {
		okey := it.Item().Key()
		// get edge ID from key
		tmp := bytes.Split(okey, []byte{0})
		eid := bytes.Join([][]byte{[]byte("e"), tmp[3]}, []byte{0})
		//log.Printf("Adding %s", string(bytes.Replace(okey, []byte{0}, []byte{' '}, -1) ) )
		del_keys = append(del_keys, okey, eid)
	}

	for it.Seek(ikey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ikey_prefix); it.Next() {
		ikey := it.Item().Key()
		// get edge ID from key
		//tmp := bytes.Split(ikey, []byte{0})
		//eid := bytes.Join( [][]byte{ []byte("e"), tmp[3] }, []byte{0} )
		del_keys = append(del_keys, ikey)
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
		e_prefix := []byte("e")
		for it.Seek(e_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), e_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			eid_tmp := bytes.Split(key_value, []byte{0})
			eid := eid_tmp[1]
			//log.Printf("EK:%#v", eid)
			pair_value := it.Item().Value()
			pair := bytes.Split(pair_value, []byte{0})
			//log.Printf("EV:%#v", pair)
			src := pair[1]
			dst := pair[2]
			if loadProp {
				okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
				data_value := badger.KVItem{}
				err := self.kv.Get(okey, &data_value)
				if err == nil {
					e := aql.Edge{}
					data := data_value.Value()
					proto.Unmarshal(data, &e)
					o <- e
				}
			} else {
				e := aql.Edge{Gid: string(eid), Src: string(src), Dst: string(dst)}
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

		ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ikey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ikey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			tmp := bytes.Split(key_value, []byte{0})
			oid := tmp[2]
			eid := tmp[3]
			okey := bytes.Join([][]byte{[]byte("o"), oid, []byte(id), eid}, []byte{0})

			data_value := badger.KVItem{}
			err := self.kv.Get(okey, &data_value)
			if err == nil {
				e := aql.Edge{}
				if loadProp {
					d := data_value.Value()
					proto.Unmarshal(d, &e)
				} else {
					e.Gid = string(eid)
					e.Src = string(oid)
					e.Dst = id
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
		}
	}()
	return o
}

func (self *BadgerGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)

		okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(okey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), okey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			//key_value := it.Item().Key()
			data_value := it.Item().Value()
			e := aql.Edge{}
			proto.Unmarshal(data_value, &e)

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

		ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(ikey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ikey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			tmp := bytes.Split(key_value, []byte{0})
			iid := tmp[1]
			oid := tmp[2]
			eid := tmp[3]

			okey := bytes.Join([][]byte{[]byte("o"), oid, iid, eid}, []byte{0})
			vkey := bytes.Join([][]byte{[]byte("v"), oid}, []byte{0})

			send := false
			if filter != nil {
				data_value := badger.KVItem{}
				err := self.kv.Get(okey, &data_value)
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

		okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(okey_prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), okey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Item().Key()
			tmp := bytes.Split(key_value, []byte{0})
			//oid := tmp[1]
			iid := tmp[2]
			//log.Printf("Vertex: %s", iid)
			//eid := tmp[3]

			vkey := bytes.Join([][]byte{[]byte("v"), iid}, []byte{0})

			send := false
			if filter != nil {
				data_value := it.Item().Value()
				e := aql.Edge{}
				proto.Unmarshal(data_value, &e)
				if filter(e) {
					send = true
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
	vkey := bytes.Join([][]byte{[]byte("v"), []byte(id)}, []byte{0})
	data_value := badger.KVItem{}
	err := self.kv.Get(vkey, &data_value)
	if err != nil {
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

func (self *BadgerGDB) GetVertexList(ctx context.Context, loadProp bool) chan aql.Vertex {
	log.Printf("GetVertexList: %s", loadProp)
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		it := self.kv.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		v_prefix := []byte("v")

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
