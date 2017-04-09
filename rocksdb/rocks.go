
package rocksdb

import (
  //"log"
  "fmt"
  "bytes"
  "github.com/bmeg/arachne/gdbi"
  "github.com/bmeg/arachne/ophion"
  "github.com/tecbot/gorocksdb"
  proto "github.com/golang/protobuf/proto"
)

type RocksArachne struct {
  db *gorocksdb.DB
  ro *gorocksdb.ReadOptions
  wo *gorocksdb.WriteOptions
  sequence int64
}


func NewRocksArachne(path string) gdbi.DBI {
  bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
  bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
  opts := gorocksdb.NewDefaultOptions()
  opts.SetBlockBasedTableFactory(bbto)
  opts.SetCreateIfMissing(true)
  db, _ := gorocksdb.OpenDb(opts, path)

  ro := gorocksdb.NewDefaultReadOptions()
  wo := gorocksdb.NewDefaultWriteOptions()
  //wo.SetSync(true)

  return &RocksArachne{db:db, ro:ro, wo:wo, sequence:0}
}


func (self *RocksArachne) Close() {
  self.db.Close()
}

func (self *RocksArachne) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self, false)
  return nil
}

func (self *RocksArachne) SetVertex(vertex ophion.Vertex) error {
  d, _ := proto.Marshal(&vertex)
  k := bytes.Join([][]byte{[]byte("v"), []byte(vertex.Gid)}, []byte{0})
  err := self.db.Put(self.wo, k, d)
  return err
}

func (self *RocksArachne) SetEdge(edge ophion.Edge) error {
  eid := fmt.Sprintf("%d", self.sequence)
  self.sequence += 1
  edge.Gid = eid
  data, _ := proto.Marshal(&edge)
  //log.Printf("SetEdge: %s %d", edge, len(data))

  src := edge.Out
  dst := edge.In
  ekey := bytes.Join([][]byte{[]byte("e"), []byte(eid)}, []byte{0})
  okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
  ikey := bytes.Join([][]byte{[]byte("i"), []byte(dst), []byte(src), []byte(eid)}, []byte{0})

  wb := gorocksdb.NewWriteBatch()
  wb.Put(ekey, okey)
  wb.Put(okey, data)
  wb.Put(ikey, []byte{})
  err := self.db.Write(self.wo, wb)
  return err
}

func (self *RocksArachne) DelEdge(eid string) error {
  ekey := bytes.Join([][]byte{[]byte("e"), []byte(eid)}, []byte{0})

  pair_value, err := self.db.Get(self.ro, ekey)
  if err != nil {
    return err
  }
  defer pair_value.Free()
  pair := bytes.Split(pair_value.Data(), []byte{0})
  src := pair[1]
  dst := pair[2]

  okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
  ikey := bytes.Join([][]byte{[]byte("i"), []byte(dst), []byte(src), []byte(eid)}, []byte{0})

  if err := self.db.Delete(self.wo, ekey); err != nil {
    return err
  }
  if err := self.db.Delete(self.wo, okey); err != nil {
    return err
  }
  if err := self.db.Delete(self.wo, ikey); err != nil {
    return err
  }
  return nil
}

func (self *RocksArachne) DelVertex(id string) error {
  vid := bytes.Join([][]byte{[]byte("v"), []byte(id)}, []byte{0})
  self.db.Delete(self.wo, vid)

  okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
  ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})

  del_keys := make([][]byte, 0, 1000)

  it := self.db.NewIterator(self.ro)
  defer it.Close()
  it.Seek(okey_prefix)
  for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
	   key := it.Key()
     okey := key.Data()
     key.Free()
     // get edge ID from key
     tmp := bytes.Split(okey, []byte{0})
     eid := bytes.Join( [][]byte{ []byte("e"), tmp[3] }, []byte{0} )
     del_keys = append(del_keys, okey, eid)
  }

  it.Seek(ikey_prefix)
  for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
    key := it.Key()
    ikey := key.Data()
    key.Free()
    // get edge ID from key
    tmp := bytes.Split(ikey, []byte{0})
    eid := bytes.Join( [][]byte{ []byte("e"), tmp[3] }, []byte{0} )
    del_keys = append(del_keys, ikey, eid)
  }

  wb := gorocksdb.NewWriteBatch()
  for _, k := range del_keys {
    wb.Delete(k)
  }
  err := self.db.Write(self.wo, wb)
  return err
}


func (self *RocksArachne) GetEdgeList() chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)
    it := self.db.NewIterator(self.ro)
    defer it.Close()
    e_prefix := []byte("e")
    it.Seek(e_prefix)
    for it = it; it.ValidForPrefix(e_prefix); it.Next() {
      key_value := it.Key()
      eid_tmp := bytes.Split(key_value.Data(), []byte{0})
      eid := eid_tmp[1]
      //log.Printf("EK:%#v", eid)
      key_value.Free()
      pair_value := it.Value()
      pair := bytes.Split(pair_value.Data(), []byte{0})
      //log.Printf("EV:%#v", pair)
      pair_value.Free()
      src := pair[1]
      dst := pair[2]
      okey := bytes.Join([][]byte{[]byte("o"), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
      data_value, err := self.db.Get(self.ro, okey)
      if err == nil {
        data := data_value.Data()
        e := ophion.Edge{}
        proto.Unmarshal(data, &e)
        //log.Printf("EP:%#v %s %d", okey, e, len(data))
        data_value.Free()
        o <- e
      }
    }
  }()
  return o
}


func (self *RocksArachne) GetInEdgeList(id string, filter gdbi.EdgeFilter) chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)

    ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})
    it := self.db.NewIterator(self.ro)
    defer it.Close()

    it.Seek(ikey_prefix)
    for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
      key_value := it.Key()
      tmp := bytes.Split(key_value.Data(), []byte{0})
      key_value.Free()
      oid := tmp[1]
      eid := tmp[2]
      okey := bytes.Join( [][]byte{ []byte("o"), oid, []byte(id), eid }, []byte{0} )

      data_value, err := self.db.Get(self.ro, okey)
      if err == nil {
        d := data_value.Data()
        e := ophion.Edge{}
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
    }
  }()
  return o
}


func (self *RocksArachne) GetOutEdgeList(id string, filter gdbi.EdgeFilter) chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)

    okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
    it := self.db.NewIterator(self.ro)
    defer it.Close()

    it.Seek(okey_prefix)
    for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
      key_value := it.Key()
      key_value.Free()
      data_value := it.Value()
      d := data_value.Data()
      e := ophion.Edge{}
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


func (self *RocksArachne) GetInList(id string, filter gdbi.EdgeFilter) chan ophion.Vertex {
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(o)

    ikey_prefix := bytes.Join([][]byte{[]byte("i"), []byte(id)}, []byte{0})
    it := self.db.NewIterator(self.ro)
    defer it.Close()

    it.Seek(ikey_prefix)
    for it = it; it.ValidForPrefix(ikey_prefix); it.Next() {
      key_value := it.Key()
      tmp := bytes.Split(key_value.Data(), []byte{0})
      key_value.Free()
      iid := tmp[1]
      oid := tmp[2]
      eid := tmp[3]

      okey := bytes.Join( [][]byte{ []byte("o"), oid, iid, eid}, []byte{0})
      vkey := bytes.Join( [][]byte{ []byte("v"), oid }, []byte{0} )

      send := false
      if filter != nil {
        data_value, err := self.db.Get(self.ro, okey)
        if err == nil {
          d := data_value.Data()
          data_value.Free()
          e := ophion.Edge{}
          proto.Unmarshal(d, &e)
          if filter(e) {
            send = true
          }
        }
      } else {
        send = true
      }
      if send {
        data_value, err := self.db.Get(self.ro, vkey)
        if err == nil {
          d := data_value.Data()
          v := ophion.Vertex{}
          proto.Unmarshal(d, &v)
          data_value.Free()
          o <- v
        }
      }
    }
  }()
  return o
}


func (self *RocksArachne) GetOutList(id string, filter gdbi.EdgeFilter) chan ophion.Vertex {
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(o)

    okey_prefix := bytes.Join([][]byte{[]byte("o"), []byte(id)}, []byte{0})
    it := self.db.NewIterator(self.ro)
    defer it.Close()

    it.Seek(okey_prefix)
    for it = it; it.ValidForPrefix(okey_prefix); it.Next() {
      key_value := it.Key()
      tmp := bytes.Split(key_value.Data(), []byte{0})
      key_value.Free()
      //oid := tmp[1]
      iid := tmp[2]
      //eid := tmp[3]

      vkey := bytes.Join( [][]byte{ []byte("v"), iid }, []byte{0} )

      send := false
      if filter != nil {
        data_value := it.Value()
        d := data_value.Data()
        data_value.Free()
        e := ophion.Edge{}
        proto.Unmarshal(d, &e)
        if filter(e) {
          send = true
        }
      } else {
        send = true
      }
      if send {
        data_value, err := self.db.Get(self.ro, vkey)
        if err == nil {
          d := data_value.Data()
          v := ophion.Vertex{}
          proto.Unmarshal(d, &v)
          data_value.Free()
          o <- v
        }
      }
    }
  }()
  return o
}



func (self *RocksArachne) GetVertex(id string) *ophion.Vertex {
  vkey := bytes.Join( [][]byte{ []byte("v"), []byte(id) }, []byte{0} )
  data_value, err := self.db.Get(self.ro, vkey)
  if err != nil {
    return nil
  }
  d := data_value.Data()
  v := ophion.Vertex{}
  proto.Unmarshal(d, &v)
  data_value.Free()

  return &v
}


func (self *RocksArachne) GetVertexList() chan ophion.Vertex {
	o := make(chan ophion.Vertex, 100)

	go func() {
		defer close(o)
    it := self.db.NewIterator(self.ro)
    defer it.Close()
    v_prefix := []byte("v")
    it.Seek(v_prefix)
    for it = it; it.ValidForPrefix(v_prefix); it.Next() {
      data_value := it.Value()
      d := data_value.Data()
      data_value.Free()
      v := ophion.Vertex{}
      proto.Unmarshal(d, &v)
      o <- v
    }
	} ()
	return o
}
