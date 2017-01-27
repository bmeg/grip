package boltdb

import (
	"bytes"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/ophion"
	"github.com/boltdb/bolt"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/struct"
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

type graphpipe func() chan ophion.QueryResult

type BoltGremlinSet struct {
	db         *BoltArachne
	readOnly   bool
	pipe       graphpipe
	sideEffect bool
	err        error
}

func (self *BoltGremlinSet) append(pipe graphpipe) *BoltGremlinSet {
	return &BoltGremlinSet{
		db:         self.db,
		readOnly:   self.readOnly,
		pipe:       pipe,
		sideEffect: self.sideEffect,
		err:        self.err,
	}
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

func (self *BoltArachne) Close() {
	self.db.Close()
}

func (self *BoltArachne) Query() gdbi.QueryInterface {
	return &BoltGremlinSet{db: self, readOnly: false, sideEffect: false, err: nil}
}

func (self *BoltArachne) setVertex(vertex ophion.Vertex) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(VertexBucket)
		d, _ := proto.Marshal(&vertex)
		//log.Printf("Putting: %s %#v %#v", vertex.Gid, vertex, d)
		b.Put([]byte(vertex.Gid), d)
		return nil
	})
	return err
}

func (self *BoltArachne) getVertexData(key string) *[]byte {
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
func (self *BoltArachne) getVertex(key string) *ophion.Vertex {
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

func (self *BoltArachne) setEdge(edge ophion.Edge) error {
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

func (self *BoltArachne) getVertexList() chan ophion.Vertex {
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

func (self *BoltArachne) getOutList(key string) chan ophion.Vertex {
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
			v := self.getVertex(i)
			if v == nil {
				//log.Printf("Vertex Missing %s", i)
			} else {
				o <- *v
			}
		}
	}()
	return o
}

func (self *BoltArachne) getInList(key string) chan ophion.Vertex {
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
			o <- *self.getVertex(i)
		}
	}()
	return o
}

func (self *BoltArachne) getEdgeList() chan ophion.Edge {
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

func (self *BoltGremlinSet) V(key ...string) gdbi.QueryInterface {
	if len(key) > 0 {
		return self.append(
			func() chan ophion.QueryResult {
				o := make(chan ophion.QueryResult, 1)
				go func() {
					defer close(o)
					v := self.db.getVertex(key[0])
					if v != nil {
						o <- ophion.QueryResult{&ophion.QueryResult_Vertex{v}}
					}
				}()
				return o
			})
	}
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 100)
			go func() {
				defer close(o)
				for i := range self.db.getVertexList() {
					t := i //make a local copy
					o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&t}}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) E() gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				log.Printf("Getting Edge List")
				for i := range self.db.getEdgeList() {
					t := i //make a local copy
					o <- ophion.QueryResult{&ophion.QueryResult_Edge{&t}}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) Has(prop string, value ...string) gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					//Process Vertex Elements
					if v := i.GetVertex(); v != nil && v.Properties != nil {
						if p, ok := v.Properties.Fields[prop]; ok {
							found := false
							for _, s := range value {
								if p.GetStringValue() == s {
									found = true
								}
							}
							if found {
								o <- i
							}
						}
					}
					//Process Edge Elements
					if e := i.GetEdge(); e != nil && e.Properties != nil {
						if p, ok := e.Properties.Fields[prop]; ok {
							found := false
							for _, s := range value {
								if p.GetStringValue() == s {
									found = true
								}
							}
							if found {
								o <- i
							}
						}
					}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) Out(key ...string) gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetVertex(); v != nil {
						for e := range self.db.getOutList(v.Gid) {
							el := e
							o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&el}}
						}
					}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) In(key ...string) gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetVertex(); v != nil {
						for e := range self.db.getInList(v.Gid) {
							el := e
							o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&el}}
						}
					}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) Property(key string, value string) gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetVertex(); v != nil {
						vl := *v //local copy
						if vl.Properties == nil {
							vl.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						vl.Properties.Fields[key] = &structpb.Value{Kind: &structpb.Value_StringValue{value}}
						o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&vl}}
					}
					if e := i.GetEdge(); e != nil {
						el := *e
						if el.Properties == nil {
							el.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						el.Properties.Fields[key] = &structpb.Value{Kind: &structpb.Value_StringValue{value}}
						o <- ophion.QueryResult{&ophion.QueryResult_Edge{&el}}
					}
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) AddV(key string) gdbi.QueryInterface {
	out := self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 1)
			o <- ophion.QueryResult{&ophion.QueryResult_Vertex{
				&ophion.Vertex{
					Gid: key,
				},
			}}
			defer close(o)
			return o
		})
	out.sideEffect = true
	return out
}

func (self *BoltGremlinSet) AddE(key string) gdbi.QueryInterface {
	out := self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if v := src.GetVertex(); v != nil {
						o <- ophion.QueryResult{&ophion.QueryResult_Edge{
							&ophion.Edge{Out: v.Gid, Label: key},
						}}
					}
				}
			}()
			return o
		})
	out.sideEffect = true
	return out
}

func (self *BoltGremlinSet) To(key string) gdbi.QueryInterface {
	out := self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if e := src.GetEdge(); e != nil {
						el := e
						el.In = key
						o <- ophion.QueryResult{&ophion.QueryResult_Edge{
							el,
						}}
					}
				}
			}()
			return o
		})
	out.sideEffect = true
	return out
}

func (self *BoltGremlinSet) Count() gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 1)
			go func() {
				defer close(o)
				var count int64 = 0
				for range self.pipe() {
					count += 1
				}
				o <- ophion.QueryResult{&ophion.QueryResult_IntValue{IntValue: count}}
			}()
			return o
		})
}

func (self *BoltGremlinSet) Limit(limit int64) gdbi.QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 1)
			go func() {
				defer close(o)
				var count int64 = 0
				//TODO: cancel the pipe once we're done with it, rather then
				//reading out the whole thing
				for i := range self.pipe() {
					if count < limit {
						o <- i
					}
					count += 1
				}
			}()
			return o
		})
}

func (self *BoltGremlinSet) Execute() chan ophion.QueryResult {
	if self.sideEffect {
		o := make(chan ophion.QueryResult, 10)
		go func() {
			defer close(o)
			for i := range self.pipe() {
				if v := i.GetVertex(); v != nil {
					self.db.setVertex(*v)
					o <- i
				} else if v := i.GetEdge(); v != nil {
					self.db.setEdge(*v)
					o <- i
				}
			}
		}()
		return o
	} else {
		return self.pipe()
	}
}

func (self *BoltGremlinSet) Run() error {
	if self.err != nil {
		return self.err
	}
	for range self.Execute() {
	}
	return nil
}

func (self *BoltGremlinSet) First() (ophion.QueryResult, error) {
	o := ophion.QueryResult{}
	if self.err != nil {
		return o, self.err
	}
	for i := range self.Execute() {
		o = i
	}
	return o, nil
}
