package gdbi

import (
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/ophion"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
)

type PipeEngine struct {
	db         DBI
	readOnly   bool
	pipe       GraphPipe
	sideEffect bool
	err        error
	selection  []string
	imports    []string
}

const (
	PIPE_SIZE = 100
)

func NewPipeEngine(db DBI, readOnly bool) *PipeEngine {
	return &PipeEngine{db: db, readOnly: readOnly, sideEffect: false, err: nil, selection: []string{}, imports: []string{}}
}

func (self *PipeEngine) append(pipe GraphPipe) *PipeEngine {
	return &PipeEngine{
		db:         self.db,
		readOnly:   self.readOnly,
		pipe:       pipe,
		sideEffect: self.sideEffect,
		err:        self.err,
		imports:    self.imports,
	}
}

func (self *PipeEngine) V(key ...string) QueryInterface {
	if len(key) > 0 {
		return self.append(
			func(request PipeRequest) chan Traveler {
				o := make(chan Traveler, PIPE_SIZE)
				go func() {
					defer close(o)
					v := self.db.GetVertex(key[0], request.LoadProperties)
					if v != nil {
						c := Traveler{}
						o <- c.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{v}})
					}
				}()
				return o
			})
	}
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetVertexList(request.LoadProperties) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetEdgeList(request.LoadProperties) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
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
		func(request PipeRequest) chan Traveler {
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
				for i := range self.pipe(request) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for ov := range self.db.GetOutList(v.Gid, request.LoadProperties, filt) {
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
		func(request PipeRequest) chan Traveler {
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
				for i := range self.pipe(request) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInList(v.Gid, request.LoadProperties, filt) {
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
		func(request PipeRequest) chan Traveler {
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
				for i := range self.pipe(request) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(v.Gid, request.LoadProperties, filt) {
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
		func(request PipeRequest) chan Traveler {
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
				for i := range self.pipe(PipeRequest{LoadProperties:false}) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInEdgeList(v.Gid, request.LoadProperties, filt) {
							el := e
							o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&el}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) As(label string) QueryInterface {
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
					o <- i.AddLabeled(label, *i.GetCurrent())
				}
			}()
			return o
		})
}

func (self *PipeEngine) Select(labels []string) QueryInterface {
	o := self.append(self.pipe)
	o.selection = labels
	return o
}

func (self *PipeEngine) Values(labels []string) QueryInterface {
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
					var props *structpb.Struct = nil
					if v := i.GetCurrent().GetVertex(); v != nil && v.Properties != nil {
						props = v.GetProperties()
					} else if v := i.GetCurrent().GetEdge(); v != nil && v.Properties != nil {
						props = v.GetProperties()
					}
					if props != nil {
						out := structpb.Struct{Fields: map[string]*structpb.Value{}}
						if len(labels) == 0 {
							protoutil.CopyStructToStruct(&out, props)
						} else {
							protoutil.CopyStructToStructSub(&out, labels, props)
						}
						o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Struct{&out}})
					}
				}
			}()
			return o
		})
	o := self.append(self.pipe)
	return o
}

func (self *PipeEngine) Import(source string) QueryInterface {
	o := self.append(self.pipe)
	o.imports = append(o.imports, source)
	return o
}

func (self *PipeEngine) Map(source string) QueryInterface {
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
					out := mfunc.Call(i.GetCurrent())
					if out != nil {
						a := i.AddCurrent(*out)
						o <- a
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) Fold(source string) QueryInterface {
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				var last *ophion.QueryResult = nil
				first := true
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
					if first {
						last = i.GetCurrent()
						first = false
					} else {
						last = mfunc.Call(last, i.GetCurrent())
					}
				}
				if last != nil {
					i := Traveler{}
					a := i.AddCurrent(*last)
					o <- a
				}
			}()
			return o
		})
}

func (self *PipeEngine) Property(key string, value interface{}) QueryInterface {
	return self.append(
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						vl := *v //local copy
						if vl.Properties == nil {
							vl.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						protoutil.StructSet(vl.Properties, key, value)
						o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Vertex{&vl}})
					}
					if e := i.GetCurrent().GetEdge(); e != nil {
						el := *e
						if el.Properties == nil {
							el.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						protoutil.StructSet(el.Properties, key, value)
						o <- i.AddCurrent(ophion.QueryResult{&ophion.QueryResult_Edge{&el}})
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) AddV(key string) QueryInterface {
	out := self.append(
		func(request PipeRequest) chan Traveler {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(PipeRequest{LoadProperties:false}) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(PipeRequest{LoadProperties:false}) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(PipeRequest{LoadProperties:false}) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, 1)
			go func() {
				defer close(o)
				var count int32 = 0
				for range self.pipe(PipeRequest{LoadProperties:false}) {
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
		func(request PipeRequest) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var count int64 = 0
				//TODO: cancel the pipe once we're done with it, rather then
				//reading out the whole thing
				for i := range self.pipe(PipeRequest{LoadProperties:true}) {
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
	if self.pipe == nil {
		return nil
	}
	if self.sideEffect {
		o := make(chan ophion.ResultRow, PIPE_SIZE)
		go func() {
			defer close(o)
			for i := range self.pipe(PipeRequest{LoadProperties:true}) {
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
			pipe := self.pipe(PipeRequest{LoadProperties:true})
			if pipe != nil {
				for i := range pipe {
					if len(self.selection) == 0 {
						o <- ophion.ResultRow{Value: i.GetCurrent()}
					} else {
						l := []*ophion.QueryResult{}
						for _, r := range self.selection {
							l = append(l, i.GetLabeled(r))
						}
						o <- ophion.ResultRow{Row: l}
					}
				}
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
