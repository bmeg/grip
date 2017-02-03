package boltdb

import (
	"bytes"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/ophion"
	"github.com/boltdb/bolt"
	proto "github.com/golang/protobuf/proto"
	//"github.com/golang/protobuf/ptypes/struct"
	"log"
)

//Outgoing edges
var OEdgeBucket = []byte("oedges")

//Incoming edges
var IEdgeBucket = []byte("iedges")

//Vertices
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
		oe := tx.Bucket(OEdgeBucket)
		ie := tx.Bucket(IEdgeBucket)
		src := edge.Out
		dst := edge.In
		okey := bytes.Join([][]byte{[]byte(src), []byte(dst)}, []byte{0})
		ikey := bytes.Join([][]byte{[]byte(dst), []byte(src)}, []byte{0})
		data, _ := proto.Marshal(&edge)
		oe.Put(okey, data)
		ie.Put(ikey, []byte{})
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

func (self *BoltArachne) GetOutList(key string) chan ophion.Vertex {
	vo := make(chan string, 100)
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(vo)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(OEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
				pair := bytes.Split(k, []byte{0})
				vo <- string(pair[1])
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

func (self *BoltArachne) GetInList(key string) chan ophion.Vertex {
	vi := make(chan string, 100)
	o := make(chan ophion.Vertex, 100)
	go func() {
		defer close(vi)
		self.db.View(func(tx *bolt.Tx) error {
			eb := tx.Bucket(IEdgeBucket)
			c := eb.Cursor()
			pre := append([]byte(key), 0)
			for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
				pair := bytes.Split(k, []byte{0})
				vi <- string(pair[1])
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
		oeb := tx.Bucket(OEdgeBucket)
		ieb := tx.Bucket(IEdgeBucket)
		
		idel := make([][]byte, 0, 100)
		c := oeb.Cursor()
		for k, _ := c.Seek([]byte(id)); bytes.HasPrefix(k, []byte(id)); k, _ = c.Next() {
			idel = append(idel, k)
		}
		
		for _, okey := range idel {
			oeb.Delete(okey)
			pair := bytes.Split(okey, []byte{0})
			ikey := bytes.Join([][]byte{[]byte(pair[1]), []byte(pair[0])}, []byte{0})
			ieb.Delete(ikey)
		}
		
		return nil
	})
	return err
}


func (self *BoltArachne) DelVertex(id string) error {
	log.Printf("del %s", id)
	err := self.db.Update(func(tx *bolt.Tx) error {
		oeb := tx.Bucket(OEdgeBucket)
		ieb := tx.Bucket(IEdgeBucket)
		vb := tx.Bucket(VertexBucket)
		
		vb.Delete([]byte(id))
		
		idel := make([][]byte, 0, 100)
		c := oeb.Cursor()
		for k, _ := c.Seek([]byte(id)); bytes.HasPrefix(k, []byte(id)); k, _ = c.Next() {
			idel = append(idel, k)
		}
		
		for _, okey := range idel {
			oeb.Delete(okey)
			pair := bytes.Split(okey, []byte{0})
			ikey := bytes.Join([][]byte{[]byte(pair[1]), []byte(pair[0])}, []byte{0})
			ieb.Delete(ikey)
		}
		
		return nil
	})
	return err
}

