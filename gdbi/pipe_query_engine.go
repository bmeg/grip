package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
)

const (
	STATE_CUSTOM          = 0
	STATE_RAW_VERTEX_LIST = 1
	STATE_RAW_EDGE_LIST   = 2
)

type PipeEngine struct {
	db         DBI
	pipe       GraphPipe
	sideEffect bool
	err        error
	selection  []string
	imports    []string
	state      int
}

const (
	PIPE_SIZE = 100
)

var PROP_LOAD string = "load"

func NewPipeEngine(db DBI) *PipeEngine {
	return &PipeEngine{
		db:         db,
		sideEffect: false,
		err:        nil,
		selection:  []string{},
		imports:    []string{},
		state:      STATE_CUSTOM,
	}
}

func (self *PipeEngine) append(pipe GraphPipe) *PipeEngine {
	return &PipeEngine{
		db:         self.db,
		pipe:       pipe,
		sideEffect: self.sideEffect,
		err:        self.err,
		selection:  self.selection,
		imports:    self.imports,
		state:      STATE_CUSTOM, //by default, state isn't passed down the operation chain
	}
}

func (self *PipeEngine) V(key ...string) QueryInterface {
	if len(key) > 0 {
		return self.append(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, PIPE_SIZE)
				go func() {
					defer close(o)
					v := self.db.GetVertex(key[0], ctx.Value(PROP_LOAD).(bool))
					if v != nil {
						c := Traveler{}
						o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
					}
				}()
				return o
			})
	}
	out := self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetVertexList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&t}})
				}
			}()
			return o
		})
	out.state = STATE_RAW_VERTEX_LIST
	return out
}

func (self *PipeEngine) E() QueryInterface {
	out := self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.db.GetEdgeList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&t}})
				}
			}()
			return o
		})
	out.state = STATE_RAW_VERTEX_LIST
	return out
}

func (self *PipeEngine) Labeled(labels ...string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				//if the 'state' is of a raw output, ie the output of query.V() or query.E(),
				//we can skip calling the upstream element and reference the index
				if self.state == STATE_RAW_VERTEX_LIST {
					for _, l := range labels {
						for id := range self.db.VertexLabelScan(ctx, l) {
							v := self.db.GetVertex(id, ctx.Value(PROP_LOAD).(bool))
							if v != nil {
								c := Traveler{}
								o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
							}
						}
					}
				} else if self.state == STATE_RAW_EDGE_LIST {
					for _, l := range labels {
						for id := range self.db.EdgeLabelScan(ctx, l) {
							v := self.db.GetEdge(id, ctx.Value(PROP_LOAD).(bool))
							if v != nil {
								c := Traveler{}
								o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{v}})
							}
						}
					}
				} else {
					for i := range self.pipe(ctx) {
						//Process Vertex Elements
						if v := i.GetCurrent().GetVertex(); v != nil {
							found := false
							for _, s := range labels {
								if v.Label == s {
									found = true
								}
							}
							if found {
								o <- i
							}
						}
						//Process Edge Elements
						if e := i.GetCurrent().GetEdge(); e != nil {
							found := false
							for _, s := range labels {
								if e.Label == s {
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

func (self *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe(ctx) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for ov := range self.db.GetOutList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							lv := ov
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&lv}})
						}
					} else if e := i.GetCurrent().GetEdge(); e != nil {
						v := self.db.GetVertex(e.To, ctx.Value(PROP_LOAD).(bool))
						o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) In(key ...string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe(ctx) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							el := e
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&el}})
						}
					} else if e := i.GetCurrent().GetEdge(); e != nil {
						v := self.db.GetVertex(e.From, ctx.Value(PROP_LOAD).(bool))
						o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) OutE(key ...string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe(ctx) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&le}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) InE(key ...string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, false)) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							el := e
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&el}})
						}
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) As(label string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
					o <- i.AddLabeled(label, *i.GetCurrent())
				}
			}()
			return o
		})
}

func (self *PipeEngine) GroupCount(label string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				groupCount := map[string]int{}
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
					var props *structpb.Struct = nil
					if v := i.GetCurrent().GetVertex(); v != nil && v.Properties != nil {
						props = v.GetProperties()
					} else if v := i.GetCurrent().GetEdge(); v != nil && v.Properties != nil {
						props = v.GetProperties()
					}
					if props != nil {
						if x, ok := props.Fields[label]; ok {
							groupCount[x.GetStringValue()] += 1 //BUG: Only supports string data
						}
					}
				}
				out := structpb.Struct{Fields: map[string]*structpb.Value{}}
				for k, v := range groupCount {
					out.Fields[k] = &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
				}
				c := Traveler{}
				o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Struct{&out}})
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
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
						o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Struct{&out}})
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				var last *aql.QueryResult = nil
				first := true
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
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

func (self *PipeEngine) Filter(source string) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
					out := mfunc.CallBool(i.GetCurrent())
					if out {
						o <- i
					}
				}
			}()
			return o
		})
}

func (self *PipeEngine) Property(key string, value interface{}) QueryInterface {
	out := self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
					if v := i.GetCurrent().GetVertex(); v != nil {
						vl := *v //local copy
						if vl.Properties == nil {
							vl.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						protoutil.StructSet(vl.Properties, key, value)
						o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&vl}})
					}
					if e := i.GetCurrent().GetEdge(); e != nil {
						el := *e
						if el.Properties == nil {
							el.Properties = &structpb.Struct{Fields: map[string]*structpb.Value{}}
						}
						protoutil.StructSet(el.Properties, key, value)
						o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&el}})
					}
				}
			}()
			return o
		})
	out.sideEffect = true
	return out
}

func (self *PipeEngine) AddV(key string) QueryInterface {
	out := self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			t := Traveler{}
			o <- t.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{
				&aql.Vertex{
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(context.WithValue(ctx, PROP_LOAD, false)) {
					if v := src.GetCurrent().GetVertex(); v != nil {
						o <- src.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{
							&aql.Edge{From: v.Gid, Label: key},
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(context.WithValue(ctx, PROP_LOAD, false)) {
					if e := src.GetCurrent().GetEdge(); e != nil {
						el := e
						el.To = key
						o <- src.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				for src := range self.pipe(context.WithValue(ctx, PROP_LOAD, false)) {
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
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, 1)
			go func() {
				defer close(o)
				var count int32 = 0
				for range self.pipe(context.WithValue(ctx, PROP_LOAD, false)) {
					count += 1
				}
				t := Traveler{}
				o <- t.AddCurrent(aql.QueryResult{&aql.QueryResult_IntValue{IntValue: count}})
			}()
			return o
		})
}

func (self *PipeEngine) Limit(limit int64) QueryInterface {
	return self.append(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				var count int64 = 0
				nctx, cancel := context.WithCancel(ctx)
				for i := range self.pipe(nctx) {
					if count < limit {
						o <- i
					} else {
						cancel()
					}
					count += 1
				}
			}()
			return o
		})
}

func (self *PipeEngine) Execute(ctx context.Context) chan aql.ResultRow {
	if self.pipe == nil {
		return nil
	}
	if self.sideEffect {
		o := make(chan aql.ResultRow, PIPE_SIZE)
		go func() {
			defer close(o)
			for i := range self.pipe(context.WithValue(ctx, PROP_LOAD, true)) {
				if v := i.GetCurrent().GetVertex(); v != nil {
					self.db.SetVertex(*v)
					o <- aql.ResultRow{Value: i.GetCurrent()}
				} else if v := i.GetCurrent().GetEdge(); v != nil {
					self.db.SetEdge(*v)
					o <- aql.ResultRow{Value: i.GetCurrent()}
				}
			}
		}()
		return o
	} else {
		o := make(chan aql.ResultRow, PIPE_SIZE)
		go func() {
			defer close(o)
			pipe := self.pipe(context.WithValue(ctx, PROP_LOAD, true))
			if pipe != nil {
				for i := range pipe {
					if len(self.selection) == 0 {
						o <- aql.ResultRow{Value: i.GetCurrent()}
					} else {
						l := []*aql.QueryResult{}
						for _, r := range self.selection {
							l = append(l, i.GetLabeled(r))
						}
						o <- aql.ResultRow{Row: l}
					}
				}
			}
		}()
		return o
	}
}

func (self *PipeEngine) Run(ctx context.Context) error {
	if self.err != nil {
		return self.err
	}
	for range self.Execute(ctx) {
	}
	return nil
}

func (self *PipeEngine) First(ctx context.Context) (aql.ResultRow, error) {
	o := aql.ResultRow{}
	if self.err != nil {
		return o, self.err
	}
	first := true
	nctx, cancel := context.WithCancel(ctx)
	for i := range self.Execute(nctx) {
		if first {
			o = i
		}
		first = false
		cancel()
	}
	return o, nil
}
