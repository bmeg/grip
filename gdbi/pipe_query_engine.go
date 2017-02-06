package gdbi

import (
	"github.com/bmeg/arachne/ophion"
	"github.com/golang/protobuf/ptypes/struct"
	//"log"
)

type PipeEngine struct {
	db         DBI
	readOnly   bool
	pipe       GraphPipe
	sideEffect bool
	err        error
}

const (
	PIPE_SIZE = 100
)

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
			func() chan Traveler {
				o := make(chan Traveler, PIPE_SIZE)
				go func() {
					defer close(o)
					v := self.db.GetVertex(key[0])
					if v != nil {
						c := Traveler{}
						o <- c.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{v}})
					}
				}()
				return o
			})
	}
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetVertexList() {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{&t}})
				}
			}()
			return o
		})
}

func (self *PipeEngine) E() QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetEdgeList() {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&t}})
				}
			}()
			return o
		})
}

func (self *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					//Process Vertex Elements
					if v := i.GetCurrent().GetVertex(); v != nil && v.Properties != nil {
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
					if e := i.GetCurrent().GetEdge(); e != nil && e.Properties != nil {
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
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e ophion.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe() {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for ov := range self.db.GetOutList(v.Gid, filt) {
							lv := ov
							o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{&lv}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) In(key ...string) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e ophion.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe() {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInList(v.Gid, filt) {
							el := e
							o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{&el}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) OutE(key ...string) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e ophion.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe() {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(v.Gid, filt) {
							le := oe
							o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&le}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) InE(key ...string) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e ophion.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe() {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInEdgeList(v.Gid, filt) {
							el := e
							o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&el}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) Property(key string, value interface{}) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe() {
					if v := i.GetCurrent().GetVertex(); v != nil {
						vl := *v //local copy
						if vl.Properties == nil {
							vl.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						StructSet(vl.Properties, key, value)
						o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{&vl}})
					}
					if e := i.GetCurrent().GetEdge(); e != nil {
						el := *e
						if el.Properties == nil {
							el.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						StructSet(el.Properties, key, value)
						o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&el}})
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) AddV(key string) QueryInterface {
	out := self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			t := Traveler{}
			o <- t.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{
				&ophion.Vertex{
					Gid: key,
				},
			}})
			defer close(o)
			return o
		})
	out.sideEffect = true
	return out
}

func (self *PipeEngine) AddE(key string) QueryInterface {
	out := self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if v := src.GetCurrent().GetVertex(); v != nil {
						o <- src.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{
							&ophion.Edge{Out: v.Gid, Label: key},
						}})
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
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if e := src.GetCurrent().GetEdge(); e != nil {
						el := e
						el.In = key
						o <- src.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{
							el,
						}})
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
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe() {
					if v := src.GetCurrent().GetVertex(); v != nil {
						self.db.DelVertex(v.Gid)
					}
					if e := src.GetCurrent().GetEdge(); e != nil {
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
		func() chan Traveler {
			o := make(chan Traveler, 1)
			go func() {
				defer close(o)
				var count int32 = 0
				for range self.pipe() {
					count += 1
				}
				t := Traveler{}
				o <- t.AddCurrent(ophion.QueryResult{&ophion.QueryResult_IntValue{IntValue: count}})
			}()
			return o
		})
}

func (self *PipeEngine) Limit(limit int64) QueryInterface {
	return self.append(
		func() chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
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

func (self *PipeEngine) Execute() chan ophion.ResultRow {
	if self.sideEffect {
		o := make(chan ophion.ResultRow, PIPE_SIZE)
		go func() {
			defer close(o)
			for i := range self.pipe() {
				if v := i.GetCurrent().GetVertex(); v != nil {
					self.db.SetVertex(*v)
					o <- ophion.ResultRow{Value: i.GetCurrent()}
				} else if v := i.GetCurrent().GetEdge(); v != nil {
					self.db.SetEdge(*v)
					o <- ophion.ResultRow{Value: i.GetCurrent()}
				}
			}
		}()
		return o
	} else {
		o := make(chan ophion.ResultRow, PIPE_SIZE)
		go func() {
			defer close(o)
			for i := range self.pipe() {
				o <- ophion.ResultRow{Value: i.GetCurrent()}
			}
		}()
		return o
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

func (self *PipeEngine) First() (ophion.ResultRow, error) {
	o := ophion.ResultRow{}
	if self.err != nil {
		return o, self.err
	}
	first := true
	for i := range self.Execute() {
		if first {
			o = i
		}
		first = false
	}
	return o, nil
}
