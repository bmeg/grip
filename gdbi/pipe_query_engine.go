package gdbi

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	_ "github.com/bmeg/arachne/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/otto" // import otto so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/v8"   // import v8 so it registers with the driver map
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/ptypes/struct"
	"log"
)

func (pengine *PipeEngine) PipeIn(context.Context, func () Pipeline) QueryInterface {
	return nil
}

func (pengine *PipeEngine) PipeOut() Pipeline {
	return nil
}

// V initilizes a pipeline engine for starting on vertices
// if len(key) > 0, then it selects only vertices with matching ids
func (pengine *PipeEngine) V(key []string) QueryInterface {
	if len(key) > 0 {
		return pengine.append(fmt.Sprintf("V (%d keys) %s", len(key), key),
			newPipeOut(
				func(ctx context.Context) chan Traveler {
					o := make(chan Traveler, pipeSize)
					go func() {
						pengine.startTimer("all")
						defer close(o)
						for _, k := range key {
							v := pengine.db.GetVertex(k, ctx.Value(propLoad).(bool))
							if v != nil {
								c := Traveler{}
								o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
							}
						}
						pengine.endTimer("all")
					}()
					return o
				},
				func() int { return StateVertexList },
				func() map[string]int { return map[string]int{} },
			))
	}
	return pengine.append("V", newPipeOut(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, pipeSize)
			go func() {
				defer close(o)
				pengine.startTimer("all")
				for i := range pengine.db.GetVertexList(ctx, ctx.Value(propLoad).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: &t}})
				}
				pengine.endTimer("all")
			}()
			return o
		},
		func() int { return StateRawVertexList },
		func() map[string]int { return map[string]int{} },
	))
}

// E initilizes a pipeline for starting on edges
func (pengine *PipeEngine) E() QueryInterface {
	return pengine.append("E", newPipeOut(
		func(ctx context.Context) chan Traveler {
			o := make(chan Traveler, pipeSize)
			go func() {
				defer close(o)
				pengine.startTimer("all")
				for i := range pengine.db.GetEdgeList(ctx, ctx.Value(propLoad).(bool)) {
					t := i //make a local copy
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &t}})
				}
				pengine.endTimer("all")
			}()
			return o
		},
		func() int { return StateRawEdgeList },
		func() map[string]int { return map[string]int{} },
	))
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

// HasID filters graph elements against a list ids
func (pengine *PipeEngine) HasID(ids ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("HasId: %s", ids),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(ctx)
				go func() {
					defer close(o)
					pengine.startTimer("all")
					if pengine.pipe.GetCurrentState() == StateVertexList || pengine.pipe.GetCurrentState() == StateRawVertexList {
						for i := range pipe {
							if v := i.GetCurrent().GetVertex(); v != nil {
								if contains(ids, v.Gid) {
									o <- i
								}
							}
						}
					} else if pengine.pipe.GetCurrentState() == StateEdgeList || pengine.pipe.GetCurrentState() == StateRawEdgeList {
						for i := range pipe {
							if e := i.GetCurrent().GetEdge(); e != nil {
								if contains(ids, e.Gid) {
									o <- i
								}
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// HasLabel filters graph elements against a list of labels
func (pengine *PipeEngine) HasLabel(labels ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("HasLabel: %s", labels),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true)) //BUG: shouldn't have to load data to get label
				go func() {
					defer close(o)
					pengine.startTimer("all")
					//if the 'state' is of a raw output, ie the output of query.V() or query.E(),
					//we can skip calling the upstream element and reference the index
					if pengine.pipe.GetCurrentState() == StateRawVertexList {
						pengine.startTimer("indexScan")
						for _, l := range labels {
							for id := range pengine.db.VertexLabelScan(ctx, l) {
								v := pengine.db.GetVertex(id, ctx.Value(propLoad).(bool))
								if v != nil {
									c := Traveler{}
									o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
								}
							}
						}
						pengine.endTimer("indexScan")
					} else if pengine.pipe.GetCurrentState() == StateRawEdgeList {
						for _, l := range labels {
							for id := range pengine.db.EdgeLabelScan(ctx, l) {
								e := pengine.db.GetEdge(id, ctx.Value(propLoad).(bool))
								if e != nil {
									c := Traveler{}
									o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: e}})
								}
							}
						}
					} else {
						for i := range pipe {
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
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Has does a comparison of field `prop` in current graph element against list of values
func (pengine *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Has: %s", prop),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					defer close(o)
					pengine.startTimer("all")
					for i := range pipe {
						//Process Vertex Elements
						if v := i.GetCurrent().GetVertex(); v != nil && v.Data != nil {
							if p, ok := v.Data.Fields[prop]; ok {
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
						if e := i.GetCurrent().GetEdge(); e != nil && e.Data != nil {
							if p, ok := e.Data.Fields[prop]; ok {
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
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Out adds a step to the pipeline that moves the travels (can be on either an edge
// or a vertex) to the vertex on the other side of an outgoing edge
func (pengine *PipeEngine) Out(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Out: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					if pengine.pipe.GetCurrentState() == StateVertexList || pengine.pipe.GetCurrentState() == StateRawVertexList {
						for i := range pipe {
							if v := i.GetCurrent().GetVertex(); v != nil {
								for ov := range pengine.db.GetOutList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
									lv := ov
									o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: &lv}})
								}
							}
						}
					} else if pengine.pipe.GetCurrentState() == StateEdgeList || pengine.pipe.GetCurrentState() == StateRawEdgeList {
						idList := make(chan string, 100)
						travelerList := make(chan Traveler, 100)
						go func() {
							defer close(idList)
							defer close(travelerList)
							for i := range pipe {
								e := i.GetCurrent().GetEdge()
								idList <- e.To
								travelerList <- i
							}
						}()
						for v := range pengine.db.GetVertexListByID(ctx, idList, ctx.Value(propLoad).(bool)) {
							i := <-travelerList
							if v != nil {
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
							}
						}
					} else {
						log.Printf("Weird State: %d", pengine.pipe.GetCurrentState())
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateVertexList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Both adds a step to the pipeline that moves the travels along both the incoming
// and outgoing edges, to the connected vertex. If the traveler is on on edge,
// it will go to the vertices on both sides of the edge.
func (pengine *PipeEngine) Both(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Both: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					if pengine.pipe.GetCurrentState() == StateVertexList || pengine.pipe.GetCurrentState() == StateRawVertexList {
						for i := range pipe {
							if v := i.GetCurrent().GetVertex(); v != nil {
								for ov := range pengine.db.GetOutList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
									lv := ov
									o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: &lv}})
								}
								for ov := range pengine.db.GetInList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
									lv := ov
									o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: &lv}})
								}
							}
						}
					} else if pengine.pipe.GetCurrentState() == StateEdgeList || pengine.pipe.GetCurrentState() == StateRawEdgeList {
						idList := make(chan string, 100)
						travelerList := make(chan Traveler, 100)
						go func() {
							defer close(idList)
							defer close(travelerList)
							for i := range pipe {
								e := i.GetCurrent().GetEdge()
								idList <- e.To
								travelerList <- i
								idList <- e.From
								travelerList <- i
							}
						}()
						for v := range pengine.db.GetVertexListByID(ctx, idList, ctx.Value(propLoad).(bool)) {
							i := <-travelerList
							if v != nil {
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
							}
						}
					} else {
						log.Printf("Weird State: %d", pengine.pipe.GetCurrentState())
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateVertexList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// In adds a step to the pipeline that moves the travels (can be on either an edge
// or a vertex) to the vertex on the other side of an incoming edge
func (pengine *PipeEngine) In(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("In: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					if pengine.pipe.GetCurrentState() == StateVertexList || pengine.pipe.GetCurrentState() == StateRawVertexList {
						for i := range pipe {
							if v := i.GetCurrent().GetVertex(); v != nil {
								for e := range pengine.db.GetInList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
									el := e
									o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: &el}})
								}
							}
						}
					} else if pengine.pipe.GetCurrentState() == StateEdgeList || pengine.pipe.GetCurrentState() == StateRawEdgeList {
						for i := range pipe {
							if e := i.GetCurrent().GetEdge(); e != nil {
								v := pengine.db.GetVertex(e.From, ctx.Value(propLoad).(bool))
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateVertexList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// OutE adds a step to the pipeline to move the travelers to the outgoing edges
// connected to a vertex
func (pengine *PipeEngine) OutE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("OutE: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					for i := range pipe {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for oe := range pengine.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
								le := oe
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateEdgeList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// BothE looks for both incoming and outgoing edges connected to the
// current vertex
func (pengine *PipeEngine) BothE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("BothE: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					for i := range pipe {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for oe := range pengine.db.GetOutEdgeList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
								le := oe
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
							}
							for oe := range pengine.db.GetInEdgeList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
								le := oe
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateEdgeList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// OutBundle adds a step in the processing pipeline to select bundles from the
// current vertex, if len(key) > 0, then the label must equal
func (pengine *PipeEngine) OutBundle(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("OutBundle: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					for i := range pipe {
						if v := i.GetCurrent().GetVertex(); v != nil {
							//log.Printf("GetEdgeList: %s", v.Gid)
							for oe := range pengine.db.GetOutBundleList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
								le := oe
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Bundle{Bundle: &le}})
							}
							//log.Printf("Done GetEdgeList: %s", v.Gid)
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateBundleList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// InE adds a step to the pipeline that moves the travelers to the incoming
// edges attached to current position if len(key) > 0 then the edge labels
// must match an entry in `key`
func (pengine *PipeEngine) InE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("InE: %s", key),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					for i := range pipe {
						if v := i.GetCurrent().GetVertex(); v != nil {
							for e := range pengine.db.GetInEdgeList(ctx, v.Gid, ctx.Value(propLoad).(bool), key) {
								el := e
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &el}})
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateEdgeList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// As marks the current graph element with `label` and stores it in the travelers
// state
func (pengine *PipeEngine) As(label string) QueryInterface {
	return pengine.append(fmt.Sprintf("As: %s", label),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					for i := range pipe {
						if i.HasLabeled(label) {
							c := i.GetLabeled(label)
							o <- i.AddCurrent(*c)
						} else {
							o <- i.AddLabeled(label, *i.GetCurrent())
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int {
				valueStates := pengine.pipe.GetValueStates()
				if _, ok := valueStates[label]; ok {
					return valueStates[label]
				}
				return pengine.pipe.GetCurrentState()
			},
			func() map[string]int {
				valueStates := pengine.pipe.GetValueStates()
				if _, ok := valueStates[label]; ok {
					return valueStates
				}
				stateMap := map[string]int{}
				for k, v := range valueStates {
					stateMap[k] = v
				}
				stateMap[label] = pengine.pipe.GetCurrentState()
				return stateMap
			},
		))
}

// GroupCount adds a step to the pipeline that does a group count for data in field
// label
func (pengine *PipeEngine) GroupCount(label string) QueryInterface {
	return pengine.append(fmt.Sprintf("GroupCount: %s", label),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					defer close(o)
					pengine.startTimer("all")
					groupCount := map[string]int{}
					for i := range pipe {
						var props *structpb.Struct
						if v := i.GetCurrent().GetVertex(); v != nil && v.Data != nil {
							props = v.GetData()
						} else if v := i.GetCurrent().GetEdge(); v != nil && v.Data != nil {
							props = v.GetData()
						}
						if props != nil {
							if x, ok := props.Fields[label]; ok {
								groupCount[x.GetStringValue()]++ //BUG: Only supports string data
							}
						}
					}
					out := map[string]interface{}{}
					for k, v := range groupCount {
						out[k] = v
					}
					c := Traveler{}
					o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Data{Data: protoutil.WrapValue(out)}})
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateCustom },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Select adds a step to the pipeline that makes the output select pull previously
// marked items
func (pengine *PipeEngine) Select(labels []string) QueryInterface {
	o := pengine.append("Select", pengine.pipe)
	o.selection = labels
	return o
}

// Values adds a step to the pipelines that takes values from the traveler's current
// state and select fields `labels`
func (pengine *PipeEngine) Values(labels []string) QueryInterface {
	return pengine.append(fmt.Sprintf("Values: %s", labels),
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					defer close(o)
					pengine.startTimer("all")
					for i := range pipe {
						var props *structpb.Struct
						if v := i.GetCurrent().GetVertex(); v != nil && v.Data != nil {
							props = v.GetData()
						} else if v := i.GetCurrent().GetEdge(); v != nil && v.Data != nil {
							props = v.GetData()
						}
						if props != nil {
							out := structpb.Struct{Fields: map[string]*structpb.Value{}}
							if len(labels) == 0 {
								protoutil.CopyStructToStruct(&out, props)
							} else {
								protoutil.CopyStructToStructSub(&out, labels, props)
							}
							o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Data{Data: protoutil.WrapValue(out)}})
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateCustom },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Import runs a javascript script to add common elements to the javascript
// runtime environment
func (pengine *PipeEngine) Import(source string) QueryInterface {
	o := pengine.append("Import", pengine.pipe)
	o.imports = append(o.imports, source)
	return o
}

// Map adds a step in the pipeline, which runs a user javascript function
// which if given the current graph element and should return a transformed dict
func (pengine *PipeEngine) Map(source string) QueryInterface {
	return pengine.append("Map",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					defer close(o)
					pengine.startTimer("all")
					mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
					if err != nil {
						log.Printf("Script Error: %s", err)
					}
					for i := range pipe {
						out := mfunc.Call(i.GetCurrent())
						if out != nil {
							a := i.AddCurrent(*out)
							o <- a
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateCustom },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Fold adds a step to the pipeline that runs a 'fold' operations across all travelers
func (pengine *PipeEngine) Fold(source string, init interface{}) QueryInterface {
	return pengine.append("Fold",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					defer close(o)
					pengine.startTimer("all")
					//log.Printf("Running %s init %s", source, init)
					mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
					if err != nil || mfunc == nil {
						log.Printf("Script Error: %s", err)
						return
					}
					foldValue := &aql.QueryResult{Result: &aql.QueryResult_Data{Data: protoutil.WrapValue(init)}}
					for i := range pipe {
						//log.Printf("Fold Value: %s", foldValue)
						foldValue = mfunc.Call(foldValue, i.GetCurrent())
					}
					if foldValue != nil {
						i := Traveler{}
						a := i.AddCurrent(*foldValue)
						o <- a
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateCustom },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Filter adds a pipeline step that runs javascript function that
// inspect the values attached to the current graph element and decides
// if it should continue by returning a boolean
func (pengine *PipeEngine) Filter(source string) QueryInterface {
	return pengine.append("Filter",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
					if err != nil {
						log.Printf("Script Error: %s", err)
					}
					for i := range pipe {
						out := mfunc.CallBool(i.GetCurrent())
						if out {
							o <- i
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// FilterValues adds a pipeline step that runs javascript function that
// should inspect traveler contents. The javascript function is passed a map
// of all previously marked values and it decides if it should continue by
// returning a boolean
func (pengine *PipeEngine) FilterValues(source string) QueryInterface {
	return pengine.append("FilterValues",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
					if err != nil {
						log.Printf("Script Error: %s", err)
					}
					for i := range pipe {
						out := mfunc.CallValueMapBool(i.State)
						if out {
							o <- i
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// VertexFromValues adds a pipeline step that runs `source` javascript that
// should return a vertex string. The travels then jumps to that vertex id
func (pengine *PipeEngine) VertexFromValues(source string) QueryInterface {
	return pengine.append("VertexFromValues",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
					if err != nil {
						log.Printf("Script Error: %s", err)
					}
					for i := range pipe {
						pengine.startTimer("javascript")
						out := mfunc.CallValueToVertex(i.State)
						pengine.endTimer("javascript")
						for _, j := range out {
							v := pengine.db.GetVertex(j, ctx.Value(propLoad).(bool))
							if v != nil {
								o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
							}
						}
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateVertexList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Count adds a step to the pipeline that takes all the incoming Travelers
// and returns the count
func (pengine *PipeEngine) Count() QueryInterface {
	return pengine.append("Count",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, 1)
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, false))
				go func() {
					pengine.startTimer("all")
					defer close(o)
					var count int32
					for range pipe {
						count++
					}
					//log.Printf("Counted: %d", count)
					trav := Traveler{}
					o <- trav.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Data{Data: protoutil.WrapValue(count)}})
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return StateVertexList },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Limit adds a filter step to the pipeline that stops after the
// `limit` elements have passed through
func (pengine *PipeEngine) Limit(limit int64) QueryInterface {
	return pengine.append("Limit",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				o := make(chan Traveler, pipeSize)
				nctx, cancel := context.WithCancel(ctx)
				pipe := pengine.pipe.Start(nctx)
				go func() {
					pengine.startTimer("all")
					defer close(o)
					var count int64
					for i := range pipe {
						if count < limit {
							o <- i
						} else {
							cancel()
						}
						count++
					}
					pengine.endTimer("all")
				}()
				return o
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}

// Match adds a matching filter to a pipeline. The match is composed of an
// array of sub pipelines
func (pengine *PipeEngine) Match(matches []*QueryInterface) QueryInterface {
	return pengine.append("Match",
		newPipeOut(
			func(ctx context.Context) chan Traveler {
				pengine.startTimer("all")
				pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
				for _, matchStep := range matches {
					pipe = (*matchStep).Chain(ctx, pipe)
				}
				pengine.endTimer("all")
				return pipe.Travelers
			},
			func() int { return stateCustom(pengine.pipe.GetCurrentState()) },
			func() map[string]int { return pengine.pipe.GetValueStates() },
		))
}
