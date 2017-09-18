package gdbi

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
	"time"
)

const (
	STATE_CUSTOM          = 0
	STATE_VERTEX_LIST     = 1
	STATE_EDGE_LIST       = 2
	STATE_RAW_VERTEX_LIST = 3
	STATE_RAW_EDGE_LIST   = 4
	STATE_BUNDLE_LIST     = 5
)

func state_custom(i int) int {
	switch i {
	case STATE_EDGE_LIST:
		return STATE_EDGE_LIST
	case STATE_VERTEX_LIST:
		return STATE_VERTEX_LIST
	case STATE_RAW_EDGE_LIST:
		return STATE_EDGE_LIST
	case STATE_RAW_VERTEX_LIST:
		return STATE_VERTEX_LIST
	default:
		return STATE_CUSTOM
	}
}

type timer interface {
	start_timer()
	end_timer()
}

func NewPipeOut(t chan Traveler, state int) PipeOut {
	return PipeOut{Travelers: t, State: state}
}

type GraphPipe func(t timer, ctx context.Context) PipeOut

type PipeEngine struct {
	name                 string
	db                   DBI
	pipe                 GraphPipe
	err                  error
	selection            []string
	imports              []string
	parent               *PipeEngine
	start_time, end_time time.Time
	input                *PipeOut
}

const (
	PIPE_SIZE = 100
)

var PROP_LOAD string = "load"

func NewPipeEngine(db DBI) *PipeEngine {
	return &PipeEngine{
		name:      "start_node",
		db:        db,
		err:       nil,
		selection: []string{},
		imports:   []string{},
		parent:    nil,
		input:     nil,
		pipe:      nil,
	}
}

func (self *PipeEngine) append(name string, pipe GraphPipe) *PipeEngine {
	return &PipeEngine{
		name:      name,
		db:        self.db,
		pipe:      pipe,
		err:       self.err,
		selection: self.selection,
		imports:   self.imports,
		parent:    self,
	}
}

func (self *PipeEngine) start_timer() {
	self.start_time = time.Now()
}

func (self *PipeEngine) end_timer() {
	self.end_time = time.Now()
}

func (self *PipeEngine) get_time() time.Duration {
	return self.end_time.Sub(self.start_time)
}

func (self *PipeEngine) start_pipe(ctx context.Context) PipeOut {
	if self.input != nil {
		//log.Printf("Using chained input")
		return *self.input
	}
	pi := self.pipe(self, ctx)
	return pi
}

func (self *PipeEngine) V(key ...string) QueryInterface {
	if len(key) > 0 {
		return self.append(fmt.Sprintf("V %s", key),
			func(t timer, ctx context.Context) PipeOut {
				o := make(chan Traveler, PIPE_SIZE)
				go func() {
					t.start_timer()
					defer close(o)
					v := self.db.GetVertex(key[0], ctx.Value(PROP_LOAD).(bool))
					if v != nil {
						c := Traveler{}
						o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
					}
					t.end_timer()
				}()
				return NewPipeOut(o, STATE_VERTEX_LIST)
			})
	}
	return self.append("V",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				for i := range self.db.GetVertexList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&t}})
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_RAW_VERTEX_LIST)
		})
}

func (self *PipeEngine) E() QueryInterface {
	return self.append("E",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				for i := range self.db.GetEdgeList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&t}})
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_RAW_VERTEX_LIST)
		})
}

func (self *PipeEngine) Labeled(labels ...string) QueryInterface {
	return self.append(fmt.Sprintf("Labeled: %s", labels),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(ctx)
			go func() {
				defer close(o)
				t.start_timer()

				//if the 'state' is of a raw output, ie the output of query.V() or query.E(),
				//we can skip calling the upstream element and reference the index
				if pipe.State == STATE_RAW_VERTEX_LIST {
					for _, l := range labels {
						for id := range self.db.VertexLabelScan(ctx, l) {
							v := self.db.GetVertex(id, ctx.Value(PROP_LOAD).(bool))
							if v != nil {
								c := Traveler{}
								o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
							}
						}
					}
				} else if pipe.State == STATE_RAW_EDGE_LIST {
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
					for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, state_custom(pipe.State))
		})
}

func (self *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return self.append(fmt.Sprintf("Has: %s", prop),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer()
				for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, state_custom(pipe.State))
		})
}

func (self *PipeEngine) Out(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("Out: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer()
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
				if pipe.State == STATE_VERTEX_LIST || pipe.State == STATE_RAW_VERTEX_LIST {
					for i := range pipe.Travelers {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for ov := range self.db.GetOutList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
								lv := ov
								o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&lv}})
							}
						}
					}
				} else if pipe.State == STATE_EDGE_LIST || pipe.State == STATE_RAW_EDGE_LIST {
					id_list := make(chan string, 100)
					traveler_list := make(chan Traveler, 100)
					go func() {
						defer close(id_list)
						defer close(traveler_list)
						for i := range pipe.Travelers {
							e := i.GetCurrent().GetEdge()
							id_list <- e.To
							traveler_list <- i
						}
					}()
					for v := range self.db.GetVertexListByID(ctx, id_list, ctx.Value(PROP_LOAD).(bool)) {
						i := <-traveler_list
						if v != nil {
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
						}
					}
				} else {
					log.Printf("Weird State")
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_VERTEX_LIST)
		})
}

func (self *PipeEngine) In(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("In: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				t.start_timer()
				defer close(o)
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
				var filt EdgeFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Edge) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_VERTEX_LIST)
		})
}

func (self *PipeEngine) OutE(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("OutE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				t.start_timer()
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
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&le}})
						}
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_EDGE_LIST)
		})
}

func (self *PipeEngine) OutBundle(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("OutBundle: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				t.start_timer()
				defer close(o)
				var filt BundleFilter = nil
				if len(key) > 0 && len(key[0]) > 0 {
					filt = func(e aql.Bundle) bool {
						if key[0] == e.Label {
							return true
						}
						return false
					}
				}
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						//log.Printf("GetEdgeList: %s", v.Gid)
						for oe := range self.db.GetOutBundleList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Bundle{&le}})
						}
						//log.Printf("Done GetEdgeList: %s", v.Gid)
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_BUNDLE_LIST)
		})
}

func (self *PipeEngine) InE(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("InE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				t.start_timer()
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
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), filt) {
							el := e
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&el}})
						}
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_EDGE_LIST)
		})
}

func (self *PipeEngine) As(label string) QueryInterface {
	return self.append(fmt.Sprintf("As: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer()
				defer close(o)
				for i := range pipe.Travelers {
					if i.HasLabeled(label) {
						c := i.GetLabeled(label)
						o <- i.AddCurrent(*c)
					} else {
						o <- i.AddLabeled(label, *i.GetCurrent())
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, pipe.State)
		})
}

func (self *PipeEngine) GroupCount(label string) QueryInterface {
	return self.append(fmt.Sprintf("GroupCount: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				groupCount := map[string]int{}
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
				for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_CUSTOM)
		})
}

func (self *PipeEngine) Select(labels []string) QueryInterface {
	o := self.append("Select", self.pipe)
	o.selection = labels
	return o
}

func (self *PipeEngine) Values(labels []string) QueryInterface {
	return self.append(fmt.Sprintf("Values: %s", labels),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
				for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_CUSTOM)
		})
}

func (self *PipeEngine) Import(source string) QueryInterface {
	o := self.append("Import", self.pipe)
	o.imports = append(o.imports, source)
	return o
}

func (self *PipeEngine) Map(source string) QueryInterface {
	return self.append("Map",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
				for i := range pipe.Travelers {
					out := mfunc.Call(i.GetCurrent())
					if out != nil {
						a := i.AddCurrent(*out)
						o <- a
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_CUSTOM)
		})
}

func (self *PipeEngine) Fold(source string) QueryInterface {
	return self.append("Fold",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer()
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				var last *aql.QueryResult = nil
				first := true
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
				for i := range pipe.Travelers {
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
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_CUSTOM)
		})
}

func (self *PipeEngine) Filter(source string) QueryInterface {
	return self.append("Filter",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer()
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					out := mfunc.CallBool(i.GetCurrent())
					if out {
						o <- i
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, state_custom(pipe.State))
		})
}

func (self *PipeEngine) FilterValues(source string) QueryInterface {
	return self.append("FilterValues",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer()
				defer close(o)
				mfunc, err := jsengine.NewFunction(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					out := mfunc.CallValueMapBool(i.State)
					if out {
						o <- i
					}
				}
				t.end_timer()
			}()
			return NewPipeOut(o, state_custom(pipe.State))
		})
}

func (self *PipeEngine) Count() QueryInterface {
	return self.append("Count",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, 1)
			go func() {
				t.start_timer()
				defer close(o)
				var count int32 = 0
				pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
				for range pipe.Travelers {
					count += 1
				}
				trav := Traveler{}
				o <- trav.AddCurrent(aql.QueryResult{&aql.QueryResult_IntValue{IntValue: count}})
				t.end_timer()
			}()
			return NewPipeOut(o, STATE_CUSTOM)
		})
}

func (self *PipeEngine) Limit(limit int64) QueryInterface {
	return self.append("Limit",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			nctx, cancel := context.WithCancel(ctx)
			pipe := self.start_pipe(nctx)
			go func() {
				t.start_timer()
				defer close(o)
				var count int64 = 0

				for i := range pipe.Travelers {
					if count < limit {
						o <- i
					} else {
						cancel()
					}
					count += 1
				}
				t.end_timer()
			}()
			return NewPipeOut(o, state_custom(pipe.State))
		})
}

func (self *PipeEngine) Match(matches []*QueryInterface) QueryInterface {
	return self.append("Match",
		func(t timer, ctx context.Context) PipeOut {
			t.start_timer()
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			for _, match_step := range matches {
				pipe = (*match_step).Chain(ctx, pipe)
			}
			t.end_timer()
			return NewPipeOut(pipe.Travelers, state_custom(pipe.State))
		})
}

func (self *PipeEngine) Execute(ctx context.Context) chan aql.ResultRow {
	if self.pipe == nil {
		return nil
	}
	o := make(chan aql.ResultRow, PIPE_SIZE)
	go func() {
		defer close(o)
		self.start_timer()
		pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
		for i := range pipe.Travelers {
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
		self.end_timer()
		log.Printf("---StartTiming---")
		for p := self; p != nil; p = p.parent {
			log.Printf("%s %s", p.name, p.get_time())
		}
		log.Printf("---EndTiming---")
	}()
	return o
}

func (self *PipeEngine) Chain(ctx context.Context, input PipeOut) PipeOut {

	o := make(chan Traveler, PIPE_SIZE)
	//log.Printf("Chaining")
	for p := self; p != nil; p = p.parent {
		if p.parent == nil {
			p.input = &input
		}
	}
	pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
	go func() {
		defer close(o)
		self.start_timer()

		count := 0
		for i := range pipe.Travelers {
			o <- i
			count++
		}
		self.end_timer()
		log.Printf("---StartTiming---")
		for p := self; p != nil; p = p.parent {
			log.Printf("%s %s", p.name, p.get_time())
		}
		log.Printf("---EndTiming Processed:%d---", count)
	}()
	return NewPipeOut(o, pipe.State)
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
