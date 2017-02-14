package boltdb

import (
	"bytes"
	"fmt"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/ophion"
	"github.com/boltdb/bolt"
	proto "github.com/golang/protobuf/proto"
	//"github.com/golang/protobuf/ptypes/struct"
	//"log"
)

//Outgoing edges
//key: src 0x00 dest 0x00 edgeid
//value: edge properties
var OEdgeBucket = []byte("oedges")

//Incoming edges
//key: dest 0x00 src 0x00 edgeid
//value: blank
var IEdgeBucket = []byte("iedges")

//Incoming edges
//key: edgeid
//value: src 0x00 dst 0x00 edgeid
var EdgeBucket = []byte("edges")

//Vertices
//key: vertex id
//value: vertex properties
var VertexBucket = []byte("vertices")

type BoltArachne struct {
	db *bolt.DB
}

func NewBoltArachne(path string) gdbi.ArachneInterface {
	db, _ := bolt.Open(path, 0600, nil)

	db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(OEdgeBucket) == nil {
			tx.CreateBucket(OEdgeBucket)
		}
		if tx.Bucket(IEdgeBucket) == nil {
			tx.CreateBucket(IEdgeBucket)
		}
		if tx.Bucket(EdgeBucket) == nil {
			tx.CreateBucket(EdgeBucket)
		}
		if tx.Bucket(VertexBucket) == nil {
			tx.CreateBucket(VertexBucket)
		}
		return nil
	})
	return &BoltArachne{
		db: db,
	}
}

func (self *BoltArachne) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self, false)
}

func (self *BoltArachne) Close() {
	self.db.Close()
}

func (self *BoltArachne) SetVertex(vertex ophion.Vertex) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(VertexBucket)
		d, _ := proto.Marshal(&vertex)
		//log.Printf("Putting: %s %#v %#v", vertex.Gid, vertex, d)
		b.Put([]byte(vertex.Gid), d)
		return nil
	})
	return err
}

func (self *BoltArachne) GetVertexData(key string) *[]byte {
	var out *[]byte = nil
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(VertexBucket)
		d := b.Get([]byte(key))
		if d == nil {
			return nil
		}
		out = &d
		return nil
	})
	if err != nil {
		return nil
	}
	return out
}
func (self *BoltArachne) GetVertex(key string) *ophion.Vertex {
	var out *ophion.Vertex = nil
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(VertexBucket)
		o := &ophion.Vertex{}
		d := b.Get([]byte(key))
		if d == nil {
			return nil
		}
		proto.Unmarshal(d, o)
		out = o
		return nil
	})
	if err != nil {
		return nil
	}
	return out
}

func (self *BoltArachne) SetEdge(edge ophion.Edge) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(EdgeBucket)
		oeb := tx.Bucket(OEdgeBucket)
		ieb := tx.Bucket(IEdgeBucket)
		src := edge.Out
		dst := edge.In
		eid_num, _ := eb.NextSequence()
		eid := fmt.Sprintf("%d", eid_num)
		edge.Gid = eid
		okey := bytes.Join([][]byte{[]byte(src), []byte(dst), []byte(eid)}, []byte{0})
		ikey := bytes.Join([][]byte{[]byte(dst), []byte(src), []byte(eid)}, []byte{0})
		data, _ := proto.Marshal(&edge)
		eb.Put([]byte(eid), okey)
		oeb.Put(okey, data)
		ieb.Put(ikey, []byte{})
		return nil
	})
	return err
}

type keyval struct {
	key   string
	value []byte
}

var NTHREAD = 5

func (self *BoltArachne) GetVertexList() chan ophion.Vertex {
	o := make(chan ophion.Vertex, 100)
	od := make(chan keyval, 100)

	//read the data out of the DB
	go func() {
		defer close(od)
		self.db.View(func(tx *bolt.Tx) error {
			vb := tx.Bucket(VertexBucket)
			c := vb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				od <- keyval{key: string(k), value: v}
			}
			return nil
		})
	}()

	//de-serialize
	closer := make(chan bool, NTHREAD)
	for i := 0; i < NTHREAD; i++ {
		go func() {
			for kv := range od {
				i := ophion.Vertex{}
				proto.Unmarshal(kv.value, &i)
				i.Gid = string(kv.key)
				o <- i
			}
			closer <- true
		}()
	}

	//close channel after done
	go func() {
		for i := 0; i < NTHREAD; i++ {
			<-closer
		}
		close(o)
	}()

	return o
}

func (self *BoltArachne) GetOutList(key string, filter gdbi.EdgeFilter) chan ophion.Vertex {
	vo := make(chan string, 100)
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(vo)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(OEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
				send := false
				if filter != nil {
					e := ophion.Edge{}
					proto.Unmarshal(v, &e)
					if filter(e) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					pair := bytes.Split(k, []byte{0})
					vo <- string(pair[1])
				}
			}
			return nil
		})
	}()
	go func() {
		defer close(o)
		for i := range vo {
			v := self.GetVertex(i)
			if v == nil {
				//log.Printf("Vertex Missing %s", i)
			} else {
				o <- *v
			}
		}
	}()
	return o
}

func (self *BoltArachne) GetInList(key string, filter gdbi.EdgeFilter) chan ophion.Vertex {
	vi := make(chan string, 100)
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(vi)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(IEdgeBucket)
			ob := tx.Bucket(OEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
				key_data := bytes.Split(k, []byte{0})

				send := false
				if filter != nil {
					e := ophion.Edge{}
					ikey := bytes.Join([][]byte{[]byte(key_data[1]), []byte(key_data[0]), []byte(key_data[2])}, []byte{0})
					d := ob.Get([]byte(ikey))
					proto.Unmarshal(d, &e)
					if filter(e) {
						send = true
					}
				} else {
					send = true
				}

				if send {
					vi <- string(key_data[1])
				}
			}
			return nil
		})
	}()
	go func() {
		defer close(o)
		for i := range vi {
			o <- *self.GetVertex(i)
		}
	}()
	return o
}

func (self *BoltArachne) GetOutEdgeList(key string, filter gdbi.EdgeFilter) chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(OEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
				send := false
				e := ophion.Edge{}
				proto.Unmarshal(v, &e)
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

func (self *BoltArachne) GetInEdgeList(key string, filter gdbi.EdgeFilter) chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(IEdgeBucket)
			ob := tx.Bucket(OEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
				key_data := bytes.Split(k, []byte{0})
				ikey := bytes.Join([][]byte{[]byte(key_data[1]), []byte(key_data[0]), []byte(key_data[2])}, []byte{0})
				d := ob.Get([]byte(ikey))
				e := ophion.Edge{}
				proto.Unmarshal(d, &e)
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

func (self *BoltArachne) GetEdgeList() chan ophion.Edge {
	o := make(chan ophion.Edge, 100)
	go func() {
		defer close(o)
		self.db.View(func(tx *bolt.Tx) error {
			vb := tx.Bucket(OEdgeBucket)
			c := vb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				e := ophion.Edge{}
				proto.Unmarshal(v, &e)
				o <- e
			}
			return nil
		})
	}()
	return o
}

func (self *BoltArachne) DelEdge(id string) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(EdgeBucket)
		oeb := tx.Bucket(OEdgeBucket)
		ieb := tx.Bucket(IEdgeBucket)

		odel := make([][]byte, 0, 100)
		c := oeb.Cursor()
		for k, _ := c.Seek([]byte(id)); bytes.HasPrefix(k, []byte(id)); k, _ = c.Next() {
			odel = append(odel, k)
		}

		for _, okey := range odel {
			key_data := bytes.Split(okey, []byte{0})
			ikey := bytes.Join([][]byte{[]byte(key_data[1]), []byte(key_data[0]), []byte(key_data[2])}, []byte{0})
			eid := key_data[2]
			eb.Delete(eid)
			oeb.Delete(okey)
			ieb.Delete(ikey)
		}

		return nil
	})
	return err
}

func (self *BoltArachne) DelVertex(id string) error {
	//log.Printf("del %s", id)
	err := self.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(EdgeBucket)
		oeb := tx.Bucket(OEdgeBucket)
		ieb := tx.Bucket(IEdgeBucket)
		vb := tx.Bucket(VertexBucket)

		vb.Delete([]byte(id))

		odel := make([][]byte, 0, 100)
		c := oeb.Cursor()
		for k, _ := c.Seek([]byte(id)); bytes.HasPrefix(k, []byte(id)); k, _ = c.Next() {
			odel = append(odel, k)
		}

		for _, okey := range odel {
			key_data := bytes.Split(okey, []byte{0})
			ikey := bytes.Join([][]byte{[]byte(key_data[1]), []byte(key_data[0]), []byte(key_data[2])}, []byte{0})
			eid := key_data[2]
			eb.Delete(eid)
			oeb.Delete(okey)
			ieb.Delete(ikey)
		}

		return nil
	})
	return err
}
