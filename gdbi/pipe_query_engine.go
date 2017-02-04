package gdbi

import (
	"github.com/bmeg/arachne/ophion"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
)

type DBI interface {
	GetVertex(key string) *ophion.Vertex
	GetVertexData(key string) *[]byte
	GetVertexList() chan ophion.Vertex
	GetEdgeList() chan ophion.Edge
	GetOutList(key string, filter EdgeFilter) chan ophion.Vertex
	GetInList(key string, filter EdgeFilter) chan ophion.Vertex
	DelVertex(key string) error
	DelEdge(key string) error
	SetVertex(vertex ophion.Vertex) error
	SetEdge(edge ophion.Edge) error
}
type EdgeFilter func(edge ophion.Edge) bool
type GraphPipe func() chan ophion.QueryResult

type PipeEngine struct {
	db         DBI
	readOnly   bool
	pipe       GraphPipe
	sideEffect bool
	err        error
}

func NewPipeEngine(db DBI, readOnly bool) *PipeEngine {
	return &PipeEngine{db: db, readOnly: readOnly, sideEffect: false, err: nil}
}

func (self *PipeEngine) append(pipe GraphPipe) *PipeEngine {
	return &PipeEngine{
		db:         self.db,
		readOnly:   self.readOnly,
		pipe:       pipe,
		sideEffect: self.sideEffect,
		err:        self.err,
	}
}

func (self *PipeEngine) V(key ...string) QueryInterface {
	if len(key) > 0 {
		return self.append(
			func() chan ophion.QueryResult {
				o := make(chan ophion.QueryResult, 1)
				go func() {
					defer close(o)
					v := self.db.GetVertex(key[0])
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
				for i := range self.db.GetVertexList() {
					t := i //make a local copy
					o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&t}}
				}
			}()
			return o
		})
}

func (self *PipeEngine) E() QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				log.Printf("Getting Edge List")
				for i := range self.db.GetEdgeList() {
					t := i //make a local copy
					o <- ophion.QueryResult{&ophion.QueryResult_Edge{&t}}
				}
			}()
			return o
		})
}

func (self *PipeEngine) Has(prop string, value ...string) QueryInterface {
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

func (self *PipeEngine) Out(key ...string) QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetVertex(); v != nil {
						for e := range self.db.GetOutList(v.Gid, nil) {
							log.Printf("Out: %s %s", key, e.Label)
							if (len(key) == 0 || len(key[0]) == 0 || key[0] == e.Label) {
								el := e
								o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&el}}
							}
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) In(key ...string) QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetVertex(); v != nil {
						for e := range self.db.GetInList(v.Gid, nil) {
							if (len(key) == 0 || len(key[0]) == 0 || key[0] == e.Label) {
								el := e
								o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&el}}
							}
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) Property(key string, value interface{}) QueryInterface {
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
						StructSet(vl.Properties, key, value)
						o <- ophion.QueryResult{&ophion.QueryResult_Vertex{&vl}}
					}
					if e := i.GetEdge(); e != nil {
						el := *e
						if el.Properties == nil {
							el.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						StructSet(el.Properties, key, value)
						o <- ophion.QueryResult{&ophion.QueryResult_Edge{&el}}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) AddV(key string) QueryInterface {
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

func (self *PipeEngine) AddE(key string) QueryInterface {
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

func (self *PipeEngine) To(key string) QueryInterface {
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

//delete incoming elements, emit nothing
func (self *PipeEngine) Drop() QueryInterface {
	out := self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 10)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if v := src.GetVertex(); v != nil {
						self.db.DelVertex(v.Gid)
					}
					if e := src.GetEdge(); e != nil {
						self.db.DelEdge(e.Gid)
					}
				}
			}()
			return o
		})
	out.sideEffect = true
	return out
}

func (self *PipeEngine) Count() QueryInterface {
	return self.append(
		func() chan ophion.QueryResult {
			o := make(chan ophion.QueryResult, 1)
			go func() {
				defer close(o)
				var count int32 = 0
				for range self.pipe() {
					count += 1
				}
				o <- ophion.QueryResult{&ophion.QueryResult_IntValue{IntValue: count}}
			}()
			return o
		})
}

func (self *PipeEngine) Limit(limit int64) QueryInterface {
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

func (self *PipeEngine) Execute() chan ophion.QueryResult {
	if self.sideEffect {
		o := make(chan ophion.QueryResult, 10)
		go func() {
			defer close(o)
			for i := range self.pipe() {
				if v := i.GetVertex(); v != nil {
					self.db.SetVertex(*v)
					o <- i
				} else if v := i.GetEdge(); v != nil {
					self.db.SetEdge(*v)
					o <- i
				}
			}
		}()
		return o
	} else {
		return self.pipe()
	}
}

func (self *PipeEngine) Run() error {
	if self.err != nil {
		return self.err
	}
	for range self.Execute() {
	}
	return nil
}

func (self *PipeEngine) First() (ophion.QueryResult, error) {
	o := ophion.QueryResult{}
	if self.err != nil {
		return o, self.err
	}
	for i := range self.Execute() {
		o = i
	}
	return o, nil
}
