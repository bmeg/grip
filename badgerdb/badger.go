
package badger_gd


import (
  "fmt"
  "bytes"
  "context"
  "github.com/bmeg/arachne/aql"
  "github.com/bmeg/arachne/gdbi"
  "github.com/dgraph-io/badger/badger"
  proto "github.com/golang/protobuf/proto"
)


type BadgerGDB struct {
  kv *badger.KV
  sequence int64
}

func NewBadgerArachne(path string) gdbi.DBI {
  opts := &badger.Options{}
  *opts = badger.DefaultOptions
  opts.Dir = path
  opts.ValueDir = path
  kv, _ := badger.NewKV(opts)
  return &BadgerGDB{kv:kv}
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
  entries[0] = &badger.Entry{ Key:  ekey, Value: okey }
  entries[1] = &badger.Entry{ Key:  okey, Value: data }
  entries[2] = &badger.Entry{ Key:  ikey, Value: []byte{} }
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

	it := self.kv.NewIterator(self.ro)
	defer it.Close()
	it.Seek(okey_prefix)
	for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
		key := it.Key()
		okey := bytes_copy(key.Data())
		key.Free()
		// get edge ID from key
		tmp := bytes.Split(okey, []byte{0})
		eid := bytes.Join([][]byte{[]byte("e"), tmp[3]}, []byte{0})
		//log.Printf("Adding %s", string(bytes.Replace(okey, []byte{0}, []byte{' '}, -1) ) )
		del_keys = append(del_keys, okey, eid)
	}

	it.Seek(ikey_prefix)
	for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
		key := it.Key()
		ikey := bytes_copy(key.Data())
		key.Free()
		// get edge ID from key
		//tmp := bytes.Split(ikey, []byte{0})
		//eid := bytes.Join( [][]byte{ []byte("e"), tmp[3] }, []byte{0} )
		del_keys = append(del_keys, ikey)
	}

	wb := gorocksdb.NewWriteBatch()
	for _, k := range del_keys {
		//log.Printf("Delete %s", string(bytes.Replace(k, []byte{0}, []byte{' '}, -1) ) )
		wb.Delete(k)
	}
	err := self.kv.Write(self.wo, wb)
	if err != nil {
		log.Printf("Del Error: %s", err)
	}
	wb.Destroy()
	return err
}

func (self *BadgerGDB) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		it := self.kv.NewIterator(self.ro)
		defer it.Close()
		e_prefix := []byte("e")
		it.Seek(e_prefix)
		for it = it; it.ValidForPrefix(e_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Key()
			eid_tmp := bytes.Split(bytes_copy(key_value.Data()), []byte{0})
			eid := eid_tmp[1]
			//log.Printf("EK:%#v", eid)
			key_value.Free()
			pair_value := it.Value()
			pair := bytes.Split(bytes_copy(pair_value.Data()), []byte{0})
			//log.Printf("EV:%#v", pair)
			pair_value.Free()
			src := pair[1]
			dst := pair[2]
			if loadProp {
				okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
				data_value, err := self.kv.Get(self.ro, okey)
				if err == nil {
					e := aql.Edge{}
					data := data_value.Data()
					proto.Unmarshal(data, &e)
					data_value.Free()
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
		it := self.kv.NewIterator(self.ro)
		defer it.Close()

		it.Seek(ikey_prefix)
		for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Key()
			tmp := bytes.Split(bytes_copy(key_value.Data()), []byte{0})
			key_value.Free()
			oid := tmp[2]
			eid := tmp[3]
			okey := bytes.Join([][]byte{[]byte("o"), oid, []byte(id), eid}, []byte{0})

			data_value, err := self.kv.Get(self.ro, okey)
			if err == nil {
				e := aql.Edge{}
				if loadProp {
					d := data_value.Data()
					proto.Unmarshal(d, &e)
					data_value.Free()
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
		it := self.kv.NewIterator(self.ro)
		defer it.Close()

		it.Seek(okey_prefix)
		for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Key()
			key_value.Free()
			data_value := it.Value()
			d := data_value.Data()
			e := aql.Edge{}
			proto.Unmarshal(d, &e)
			data_value.Free()

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
		it := self.kv.NewIterator(self.ro)
		defer it.Close()

		it.Seek(ikey_prefix)
		for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Key()
			tmp := bytes.Split(bytes_copy(key_value.Data()), []byte{0})
			key_value.Free()
			iid := tmp[1]
			oid := tmp[2]
			eid := tmp[3]

			okey := bytes.Join([][]byte{[]byte("o"), oid, iid, eid}, []byte{0})
			vkey := bytes.Join([][]byte{[]byte("v"), oid}, []byte{0})

			send := false
			if filter != nil {
				data_value, err := self.kv.Get(self.ro, okey)
				if err == nil {
					d := data_value.Data()
					e := aql.Edge{}
					proto.Unmarshal(d, &e)
					data_value.Free()
					if filter(e) {
						send = true
					}
				}
			} else {
				send = true
			}
			if send {
				data_value, err := self.kv.Get(self.ro, vkey)
				if err == nil {
					d := data_value.Data()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					data_value.Free()
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
		it := self.kv.NewIterator(self.ro)
		defer it.Close()

		it.Seek(okey_prefix)
		for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key_value := it.Key()
			tmp := bytes.Split(bytes_copy(key_value.Data()), []byte{0})
			key_value.Free()
			//oid := tmp[1]
			iid := tmp[2]
			//log.Printf("Vertex: %s", iid)
			//eid := tmp[3]

			vkey := bytes.Join([][]byte{[]byte("v"), iid}, []byte{0})

			send := false
			if filter != nil {
				data_value := it.Value()
				d := data_value.Data()
				e := aql.Edge{}
				proto.Unmarshal(d, &e)
				data_value.Free()
				if filter(e) {
					send = true
				}
			} else {
				send = true
			}
			if send {
				data_value, err := self.kv.Get(self.ro, vkey)
				if err == nil {
					d := data_value.Data()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					data_value.Free()
					o <- v
				}
			}
		}
	}()
	return o
}

func (self *BadgerGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := bytes.Join([][]byte{[]byte("v"), []byte(id)}, []byte{0})
	data_value, err := self.kv.Get(self.ro, vkey)
	if err != nil {
		return nil
	}
	v := aql.Vertex{}
	if loadProp {
		d := data_value.Data()
		proto.Unmarshal(d, &v)
		data_value.Free()
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
		it := self.kv.NewIterator(self.ro)
		defer it.Close()
		v_prefix := []byte("v")
		it.Seek(v_prefix)
		for it = it; it.ValidForPrefix(v_prefix); it.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := aql.Vertex{}
			if loadProp {
				data_value := it.Value()
				d := data_value.Data()
				proto.Unmarshal(d, &v)
				data_value.Free()
			} else {
				key_value := it.Key()
				tmp := bytes.Split(bytes_copy(key_value.Data()), []byte{0})
				iid := tmp[1]
				v.Gid = string(iid)
				key_value.Free()
			}
			o <- v
		}
	}()
	return o
}
