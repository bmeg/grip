package gdbi

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	_ "github.com/bmeg/arachne/jsengine/goja"
	_ "github.com/bmeg/arachne/jsengine/otto"
	_ "github.com/bmeg/arachne/jsengine/v8"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
	"strings"
	"sync"
	"time"
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
	start_timer(label string)
	end_timer(label string)
}

func NewPipeOut(t chan Traveler, state int, valueStates map[string]int) PipeOut {
	return PipeOut{Travelers: t, State: state, ValueStates: valueStates}
}

type GraphPipe func(t timer, ctx context.Context) PipeOut

type PipeEngine struct {
	name        string
	db          DBI
	pipe        GraphPipe
	err         error
	selection   []string
	imports     []string
	parent      *PipeEngine
	start_time  map[string]time.Time
	timing      map[string]time.Duration
	timing_lock sync.Mutex
	input       *PipeOut
}

const (
	PIPE_SIZE = 100
)

var PROP_LOAD string = "load"

func NewPipeEngine(db DBI) *PipeEngine {
	return &PipeEngine{
		name:       "start_node",
		db:         db,
		err:        nil,
		selection:  []string{},
		imports:    []string{},
		parent:     nil,
		input:      nil,
		pipe:       nil,
		start_time: map[string]time.Time{},
		timing:     map[string]time.Duration{},
	}
}

func (self *PipeEngine) append(name string, pipe GraphPipe) *PipeEngine {
	return &PipeEngine{
		name:       name,
		db:         self.db,
		pipe:       pipe,
		err:        self.err,
		selection:  self.selection,
		imports:    self.imports,
		parent:     self,
		start_time: map[string]time.Time{},
		timing:     map[string]time.Duration{},
	}
}

func (self *PipeEngine) start_timer(label string) {
	self.timing_lock.Lock()
	self.start_time[label] = time.Now()
	self.timing_lock.Unlock()
}

func (self *PipeEngine) end_timer(label string) {
	self.timing_lock.Lock()
	t := time.Now().Sub(self.start_time[label])
	if o, ok := self.timing[label]; ok {
		self.timing[label] = o + t
	} else {
		self.timing[label] = t
	}
	self.timing_lock.Unlock()
}

func (self *PipeEngine) get_time() string {
	self.timing_lock.Lock()
	o := []string{}
	for k, v := range self.timing {
		o = append(o, fmt.Sprintf("%s:%s", k, v))
	}
	self.timing_lock.Unlock()
	return fmt.Sprintf("[%s]", strings.Join(o, ","))
}

func (self *PipeEngine) start_pipe(ctx context.Context) PipeOut {
	if self.input != nil {
		//log.Printf("Using chained input")
		return *self.input
	}
	pi := self.pipe(self, ctx)
	return pi
}

func (self *PipeEngine) V(key []string) QueryInterface {
	if len(key) > 0 {
		return self.append(fmt.Sprintf("V (%d keys) %s", len(key), key),
			func(t timer, ctx context.Context) PipeOut {
				o := make(chan Traveler, PIPE_SIZE)
				go func() {
					t.start_timer("all")
					defer close(o)
					for _, k := range key {
						v := self.db.GetVertex(k, ctx.Value(PROP_LOAD).(bool))
						if v != nil {
							c := Traveler{}
							o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
						}
					}
					t.end_timer("all")
				}()
				return NewPipeOut(o, STATE_VERTEX_LIST, map[string]int{})
			})
	}
	return self.append("V",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer("all")
				for i := range self.db.GetVertexList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&t}})
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_RAW_VERTEX_LIST, map[string]int{})
		})
}

func (self *PipeEngine) E() QueryInterface {
	return self.append("E",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			go func() {
				defer close(o)
				t.start_timer("all")
				for i := range self.db.GetEdgeList(ctx, ctx.Value(PROP_LOAD).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&t}})
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_RAW_VERTEX_LIST, map[string]int{})
		})
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

func (self *PipeEngine) HasId(ids ...string) QueryInterface {
	return self.append(fmt.Sprintf("HasId: %s", ids),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(ctx)
			go func() {
				defer close(o)
				t.start_timer("all")
				if pipe.State == STATE_VERTEX_LIST || pipe.State == STATE_RAW_VERTEX_LIST {
					for i := range pipe.Travelers {
						if v := i.GetCurrent().GetVertex(); v != nil {
							if contains(ids, v.Gid) {
								o <- i
							}
						}
					}
				} else if pipe.State == STATE_EDGE_LIST || pipe.State == STATE_RAW_EDGE_LIST {
					for i := range pipe.Travelers {
						if e := i.GetCurrent().GetEdge(); e != nil {
							if contains(ids, e.Gid) {
								o <- i
							}
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) HasLabel(labels ...string) QueryInterface {
	return self.append(fmt.Sprintf("HasLabel: %s", labels),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true)) //BUG: shouldn't have to load data to get label
			go func() {
				defer close(o)
				t.start_timer("all")

				//if the 'state' is of a raw output, ie the output of query.V() or query.E(),
				//we can skip calling the upstream element and reference the index
				if pipe.State == STATE_RAW_VERTEX_LIST {
					t.start_timer("indexScan")
					for _, l := range labels {
						for id := range self.db.VertexLabelScan(ctx, l) {
							v := self.db.GetVertex(id, ctx.Value(PROP_LOAD).(bool))
							if v != nil {
								c := Traveler{}
								o <- c.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
							}
						}
					}
					t.end_timer("indexScan")
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return self.append(fmt.Sprintf("Has: %s", prop),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer("all")
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) Out(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("Out: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				if pipe.State == STATE_VERTEX_LIST || pipe.State == STATE_RAW_VERTEX_LIST {
					for i := range pipe.Travelers {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for ov := range self.db.GetOutList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
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
					log.Printf("Weird State: %s", pipe.State)
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_VERTEX_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) Both(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("Both: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				if pipe.State == STATE_VERTEX_LIST || pipe.State == STATE_RAW_VERTEX_LIST {
					for i := range pipe.Travelers {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for ov := range self.db.GetOutList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
								lv := ov
								o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&lv}})
							}
							for ov := range self.db.GetInList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
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
							id_list <- e.From
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
					log.Printf("Weird State: %s", pipe.State)
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_VERTEX_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) In(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("In: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				if pipe.State == STATE_VERTEX_LIST || pipe.State == STATE_RAW_VERTEX_LIST {
					for i := range pipe.Travelers {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for e := range self.db.GetInList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
								el := e
								o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{&el}})
							}
						}
					}
				} else if pipe.State == STATE_EDGE_LIST || pipe.State == STATE_RAW_EDGE_LIST {
					for i := range pipe.Travelers {
						if e := i.GetCurrent().GetEdge(); e != nil {
							v := self.db.GetVertex(e.From, ctx.Value(PROP_LOAD).(bool))
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_VERTEX_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) OutE(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("OutE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&le}})
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_EDGE_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) BothE(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("BothE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for oe := range self.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&le}})
						}
						for oe := range self.db.GetInEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&le}})
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_EDGE_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) OutBundle(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("OutBundle: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						//log.Printf("GetEdgeList: %s", v.Gid)
						for oe := range self.db.GetOutBundleList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
							le := oe
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Bundle{&le}})
						}
						//log.Printf("Done GetEdgeList: %s", v.Gid)
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_BUNDLE_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) InE(key ...string) QueryInterface {
	return self.append(fmt.Sprintf("InE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				for i := range pipe.Travelers {
					if v := i.GetCurrent().GetVertex(); v != nil {
						for e := range self.db.GetInEdgeList(ctx, v.Gid, ctx.Value(PROP_LOAD).(bool), key) {
							el := e
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Edge{&el}})
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_EDGE_LIST, pipe.ValueStates)
		})
}

func (self *PipeEngine) As(label string) QueryInterface {
	return self.append(fmt.Sprintf("As: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer("all")
				defer close(o)
				for i := range pipe.Travelers {
					if i.HasLabeled(label) {
						c := i.GetLabeled(label)
						o <- i.AddCurrent(*c)
					} else {
						o <- i.AddLabeled(label, *i.GetCurrent())
					}
				}
				t.end_timer("all")
			}()
			if _, ok := pipe.ValueStates[label]; ok {
				return NewPipeOut(o, pipe.ValueStates[label], pipe.ValueStates)
			} else {
				stateMap := map[string]int{}
				for k, v := range pipe.ValueStates {
					stateMap[k] = v
				}
				stateMap[label] = pipe.State
				return NewPipeOut(o, pipe.State, stateMap)
			}
		})
}

func (self *PipeEngine) GroupCount(label string) QueryInterface {
	return self.append(fmt.Sprintf("GroupCount: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer("all")
				groupCount := map[string]int{}
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_CUSTOM, pipe.ValueStates)
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
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer("all")
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_CUSTOM, pipe.ValueStates)
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
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer("all")
				mfunc, err := jsengine.NewJSEngine(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					out := mfunc.Call(i.GetCurrent())
					if out != nil {
						a := i.AddCurrent(*out)
						o <- a
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_CUSTOM, pipe.ValueStates)
		})
}

func (self *PipeEngine) Fold(source string) QueryInterface {
	return self.append("Fold",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				defer close(o)
				t.start_timer("all")
				mfunc, err := jsengine.NewJSEngine(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				var last *aql.QueryResult = nil
				first := true
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_CUSTOM, pipe.ValueStates)
		})
}

func (self *PipeEngine) Filter(source string) QueryInterface {
	return self.append("Filter",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer("all")
				defer close(o)
				mfunc, err := jsengine.NewJSEngine(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					out := mfunc.CallBool(i.GetCurrent())
					if out {
						o <- i
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) FilterValues(source string) QueryInterface {
	return self.append("FilterValues",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer("all")
				defer close(o)
				mfunc, err := jsengine.NewJSEngine(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					out := mfunc.CallValueMapBool(i.State)
					if out {
						o <- i
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) VertexFromValues(source string) QueryInterface {
	return self.append("VertexFromValues",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			go func() {
				t.start_timer("all")
				defer close(o)
				mfunc, err := jsengine.NewJSEngine(source, self.imports)
				if err != nil {
					log.Printf("Script Error: %s", err)
				}
				for i := range pipe.Travelers {
					t.start_timer("javascript")
					out := mfunc.CallValueToVertex(i.State)
					t.end_timer("javascript")
					for _, j := range out {
						v := self.db.GetVertex(j, ctx.Value(PROP_LOAD).(bool))
						if v != nil {
							o <- i.AddCurrent(aql.QueryResult{&aql.QueryResult_Vertex{v}})
						}
					}
				}
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) Count() QueryInterface {
	return self.append("Count",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, 1)
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, false))
			go func() {
				t.start_timer("all")
				defer close(o)
				var count int32 = 0
				for range pipe.Travelers {
					count += 1
				}
				//log.Printf("Counted: %d", count)
				trav := Traveler{}
				o <- trav.AddCurrent(aql.QueryResult{&aql.QueryResult_IntValue{IntValue: count}})
				t.end_timer("all")
			}()
			return NewPipeOut(o, STATE_CUSTOM, pipe.ValueStates)
		})
}

func (self *PipeEngine) Limit(limit int64) QueryInterface {
	return self.append("Limit",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, PIPE_SIZE)
			nctx, cancel := context.WithCancel(ctx)
			pipe := self.start_pipe(nctx)
			go func() {
				t.start_timer("all")
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
				t.end_timer("all")
			}()
			return NewPipeOut(o, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) Match(matches []*QueryInterface) QueryInterface {
	return self.append("Match",
		func(t timer, ctx context.Context) PipeOut {
			t.start_timer("all")
			pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
			for _, match_step := range matches {
				pipe = (*match_step).Chain(ctx, pipe)
			}
			t.end_timer("all")
			return NewPipeOut(pipe.Travelers, state_custom(pipe.State), pipe.ValueStates)
		})
}

func (self *PipeEngine) Execute(ctx context.Context) chan aql.ResultRow {
	if self.pipe == nil {
		return nil
	}
	o := make(chan aql.ResultRow, PIPE_SIZE)
	go func() {
		defer close(o)
		//self.start_timer("all")
		var client time.Duration = 0
		count := 0
		pipe := self.start_pipe(context.WithValue(ctx, PROP_LOAD, true))
		for i := range pipe.Travelers {
			if len(self.selection) == 0 {
				ct := time.Now()
				o <- aql.ResultRow{Value: i.GetCurrent()}
				client += time.Now().Sub(ct)
			} else {
				l := []*aql.QueryResult{}
				for _, r := range self.selection {
					l = append(l, i.GetLabeled(r))
				}
				ct := time.Now()
				o <- aql.ResultRow{Row: l}
				client += time.Now().Sub(ct)
			}
			count++
		}
		//self.end_timer("all")
		if client > 1*time.Second { //only report timing if query takes longer then a second
			log.Printf("---StartTiming---")
			for p := self; p != nil; p = p.parent {
				log.Printf("%s %s", p.name, p.get_time())
			}
			log.Printf("---EndTiming, Processed: %d, Client wait %s---", count, client)
		}
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
		self.start_timer("all")

		count := 0
		for i := range pipe.Travelers {
			o <- i
			count++
		}
		self.end_timer("all")
		log.Printf("---StartTiming---")
		for p := self; p != nil; p = p.parent {
			log.Printf("%s %s", p.name, p.get_time())
		}
		log.Printf("---EndTiming Processed:%d---", count)
	}()
	return NewPipeOut(o, pipe.State, pipe.ValueStates)
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
