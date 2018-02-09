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
	"strings"
	"sync"
	"time"
)

type pipeOutputType int

// These consts mark the type of a PipeOut traveler chan
const (
	// outputCustom The PipeOut will be emitting custom data structures
	outputCustom pipeOutputType = iota
	// outputVertexList The PipeOut will be emitting a list of vertices
	outputVertexList
	// outputEdgeList The PipeOut will be emitting a list of edges
	outputEdgeList
	// outputRawVertexList The PipeOut will be emitting a list of all vertices, if there is an index
	// based filter, you can use skip listening and use that
	outputRawVertexList
	// outputRawEdgeList The PipeOut will be emitting a list of all edges, if there is an index
	// based filter, you can use skip listening and use that
	outputRawEdgeList
	// outputBundleList the PipeOut will be emittign a list of bundles
	outputBundleList
)

// WTF is stateCustom?
func stateCustom(i pipeOutputType) pipeOutputType {
	switch i {
	case outputEdgeList:
		return outputEdgeList
	case outputVertexList:
		return outputVertexList
	case outputRawEdgeList:
		return outputEdgeList
	case outputRawVertexList:
		return outputVertexList
	default:
		return outputCustom
	}
}

type timer interface {
	startTimer(label string)
	endTimer(label string)
}

// PipeOut represents the output of a single pipeline chain
type PipeOut struct {
	Travelers   chan Traveler
	OutputType       pipeOutputType
	ValueStates map[string]int
}

func newPipeOut(t chan Traveler, ty pipeOutputType, valueStates map[string]int) PipeOut {
	return PipeOut{Travelers: t, OutputType: ty, ValueStates: valueStates}
}

type graphPipe func(t timer, ctx context.Context) PipeOut

// PipeEngine allows the construction of a chain evaluation steps in a query pipeline
// and then will execute a complex query and filter on a graph database interface
type PipeEngine struct {
	name       string
	db         DBI
	pipe       graphPipe
	err        error
	selection  []string
	imports    []string
	parent     *PipeEngine
	startTime  map[string]time.Time
	timing     map[string]time.Duration
	timingLock sync.Mutex
	input      *PipeOut
}

const (
	pipeSize = 100
)

// NewPipeEngine creates a new PipeEngine based on the provided DBI
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
		startTime: map[string]time.Time{},
		timing:    map[string]time.Duration{},
	}
}

func (pengine *PipeEngine) append(name string, pipe graphPipe) *PipeEngine {
	return &PipeEngine{
		name:      name,
		db:        pengine.db,
		pipe:      pipe,
		err:       pengine.err,
		selection: pengine.selection,
		imports:   pengine.imports,
		parent:    pengine,
		startTime: map[string]time.Time{},
		timing:    map[string]time.Duration{},
	}
}

func (pengine *PipeEngine) startTimer(label string) {
	pengine.timingLock.Lock()
	pengine.startTime[label] = time.Now()
	pengine.timingLock.Unlock()
}

func (pengine *PipeEngine) endTimer(label string) {
	pengine.timingLock.Lock()
	t := time.Now().Sub(pengine.startTime[label])
	if o, ok := pengine.timing[label]; ok {
		pengine.timing[label] = o + t
	} else {
		pengine.timing[label] = t
	}
	pengine.timingLock.Unlock()
}

func (pengine *PipeEngine) getTime() string {
	pengine.timingLock.Lock()
	o := []string{}
	for k, v := range pengine.timing {
		o = append(o, fmt.Sprintf("%s:%s", k, v))
	}
	pengine.timingLock.Unlock()
	return fmt.Sprintf("[%s]", strings.Join(o, ","))
}

func (pengine *PipeEngine) startPipe(ctx context.Context) PipeOut {
	if pengine.input != nil {
		//log.Printf("Using chained input")
		return *pengine.input
	}
	pi := pengine.pipe(pengine, ctx)
	return pi
}

// V initilizes a pipeline engine for starting on vertices
// if len(key) > 0, then it selects only vertices with matching ids
func (pengine *PipeEngine) V(key []string) QueryInterface {
	if len(key) > 0 {
		return pengine.append(fmt.Sprintf("V (%d keys) %s", len(key), key),
			func(t timer, ctx context.Context) PipeOut {
				o := make(chan Traveler, pipeSize)
				go func() {
					t.startTimer("all")
					defer close(o)
					t.endTimer("all")
				}()
				return newPipeOut(o, outputVertexList, map[string]int{})
			})
	}
	return pengine.append("V",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputRawVertexList, map[string]int{})
		})
}

// E initilizes a pipeline for starting on edges
func (pengine *PipeEngine) E() QueryInterface {
	return pengine.append("E",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputRawEdgeList, map[string]int{})
		})
}

// HasID filters graph elements against a list ids
func (pengine *PipeEngine) HasID(ids ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("HasId: %s", ids),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(ctx)
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// HasLabel filters graph elements against a list of labels
func (pengine *PipeEngine) HasLabel(labels ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("HasLabel: %s", labels),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true)) //BUG: shouldn't have to load data to get label
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// Has does a comparison of field `prop` in current graph element against list of values
func (pengine *PipeEngine) Has(prop string, value ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Has: %s", prop),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// Out adds a step to the pipeline that moves the travels (can be on either an edge
// or a vertex) to the vertex on the other side of an outgoing edge
func (pengine *PipeEngine) Out(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Out: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputVertexList, pipe.ValueStates)
		})
}

// Both adds a step to the pipeline that moves the travels along both the incoming
// and outgoing edges, to the connected vertex. If the traveler is on on edge,
// it will go to the vertices on both sides of the edge.
func (pengine *PipeEngine) Both(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("Both: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputVertexList, pipe.ValueStates)
		})
}

// In adds a step to the pipeline that moves the travels (can be on either an edge
// or a vertex) to the vertex on the other side of an incoming edge
func (pengine *PipeEngine) In(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("In: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputVertexList, pipe.ValueStates)
		})
}

// OutE adds a step to the pipeline to move the travelers to the outgoing edges
// connected to a vertex
func (pengine *PipeEngine) OutE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("OutE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputEdgeList, pipe.ValueStates)
		})
}

// BothE looks for both incoming and outgoing edges connected to the
// current vertex
func (pengine *PipeEngine) BothE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("BothE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputEdgeList, pipe.ValueStates)
		})
}

// OutBundle adds a step in the processing pipeline to select bundles from the
// current vertex, if len(key) > 0, then the label must equal
func (pengine *PipeEngine) OutBundle(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("OutBundle: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputBundleList, pipe.ValueStates)
		})
}

// InE adds a step to the pipeline that moves the travelers to the incoming
// edges attached to current position if len(key) > 0 then the edge labels
// must match an entry in `key`
func (pengine *PipeEngine) InE(key ...string) QueryInterface {
	return pengine.append(fmt.Sprintf("InE: %s", key),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputEdgeList, pipe.ValueStates)
		})
}

// As marks the current graph element with `label` and stores it in the travelers
// state
func (pengine *PipeEngine) As(label string) QueryInterface {
	return pengine.append(fmt.Sprintf("As: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			if _, ok := pipe.ValueStates[label]; ok {
				return newPipeOut(o, pipe.ValueStates[label], pipe.ValueStates)
			}

			stateMap := map[string]int{}
			for k, v := range pipe.ValueStates {
				stateMap[k] = v
			}
			stateMap[label] = pipe.State
			return newPipeOut(o, pipe.State, stateMap)

		})
}

// GroupCount adds a step to the pipeline that does a group count for data in field
// label
func (pengine *PipeEngine) GroupCount(label string) QueryInterface {
	return pengine.append(fmt.Sprintf("GroupCount: %s", label),
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputCustom, pipe.ValueStates)
		})
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
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputCustom, pipe.ValueStates)
		})
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
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputCustom, pipe.ValueStates)
		})
}

// Fold adds a step to the pipeline that runs a 'fold' operations across all travelers
func (pengine *PipeEngine) Fold(source string) QueryInterface {
	return pengine.append("Fold",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				defer close(o)
				t.startTimer("all")
				t.endTimer("all")
			}()
			return newPipeOut(o, outputCustom, pipe.ValueStates)
		})
}

// Filter adds a pipeline step that runs javascript function that
// inspect the values attached to the current graph element and decides
// if it should continue by returning a boolean
func (pengine *PipeEngine) Filter(source string) QueryInterface {
	return pengine.append("Filter",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// FilterValues adds a pipeline step that runs javascript function that
// should inspect traveler contents. The javascript function is passed a map
// of all previously marked values and it decides if it should continue by
// returning a boolean
func (pengine *PipeEngine) FilterValues(source string) QueryInterface {
	return pengine.append("FilterValues",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// VertexFromValues adds a pipeline step that runs `source` javascript that
// should return a vertex string. The travels then jumps to that vertex id
func (pengine *PipeEngine) VertexFromValues(source string) QueryInterface {
	return pengine.append("VertexFromValues",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// Count adds a step to the pipeline that takes all the incoming Travelers
// and returns the count
func (pengine *PipeEngine) Count() QueryInterface {
	return pengine.append("Count",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, 1)
			pipe := pengine.startPipe(context.WithValue(ctx, propLoad, false))
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, outputCustom, pipe.ValueStates)
		})
}

// Limit adds a filter step to the pipeline that stops after the
// `limit` elements have passed through
func (pengine *PipeEngine) Limit(limit int64) QueryInterface {
	return pengine.append("Limit",
		func(t timer, ctx context.Context) PipeOut {
			o := make(chan Traveler, pipeSize)
			nctx, cancel := context.WithCancel(ctx)
			pipe := pengine.startPipe(nctx)
			go func() {
				t.startTimer("all")
				defer close(o)
				t.endTimer("all")
			}()
			return newPipeOut(o, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// Match adds a matching filter to a pipeline. The match is composed of an
// array of sub pipelines
func (pengine *PipeEngine) Match(matches []*QueryInterface) QueryInterface {
	return pengine.append("Match",
		func(t timer, ctx context.Context) PipeOut {
			t.startTimer("all")
			t.endTimer("all")
			return newPipeOut(pipe.Travelers, stateCustom(pipe.State), pipe.ValueStates)
		})
}

// Execute runs the current Pipeline engine
func (pengine *PipeEngine) Execute(ctx context.Context) chan aql.ResultRow {
	if pengine.pipe == nil {
    // TODO a nil channel would block forever. should return an error,
    // panic, or a closed channel. Probably panic.
		return nil
	}

  // TODO should this start a database transaction for the length of the traversal?
  // TODO think about wrapping database in memory cache layer, to make vertex/edge
  //      lookups fast? Does it matter when it's bolt/badger?

	o := make(chan aql.ResultRow, pipeSize)
	go func() {
		defer close(o)
		//pengine.startTimer("all")
		startTime := time.Now()
		var client time.Duration
		count := 0
		pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
		for i := range pipe.Travelers {
			if len(pengine.selection) == 0 {
				ct := time.Now()
				o <- aql.ResultRow{Value: i.GetCurrent()}
				client += time.Now().Sub(ct)
			} else {
				l := []*aql.QueryResult{}
				for _, r := range pengine.selection {
					l = append(l, i.GetLabeled(r))
				}
				ct := time.Now()
				o <- aql.ResultRow{Row: l}
				client += time.Now().Sub(ct)
			}
			count++
		}
		//pengine.endTimer("all")
		if time.Now().Sub(startTime) > 1*time.Second { //only report timing if query takes longer then a second
			log.Printf("---StartTiming---")
			for p := pengine; p != nil; p = p.parent {
				log.Printf("%s %s", p.name, p.getTime())
			}
			log.Printf("---EndTiming, Processed: %d, Client wait %s---", count, client)
		}
	}()
	return o
}

// Chain runs a sub pipeline, that takes and from another pipeline
func (pengine *PipeEngine) Chain(ctx context.Context, input PipeOut) PipeOut {

	o := make(chan Traveler, pipeSize)
	//log.Printf("Chaining")
	for p := pengine; p != nil; p = p.parent {
		if p.parent == nil {
			p.input = &input
		}
	}
	pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
	go func() {
		defer close(o)
		pengine.startTimer("all")

		count := 0
		for i := range pipe.Travelers {
			o <- i
			count++
		}
		pengine.endTimer("all")
		if pengine.timing["all"] > 1*time.Second {
			log.Printf("---StartTiming---")
			for p := pengine; p != nil; p = p.parent {
				log.Printf("%s %s", p.name, p.getTime())
			}
			log.Printf("---EndTiming Processed:%d---", count)
		}
	}()
	return newPipeOut(o, pipe.State, pipe.ValueStates)
}

// First runs PipeEngine process, obtains the first item, and then cancels the request
func (pengine *PipeEngine) First(ctx context.Context) (aql.ResultRow, error) {
	o := aql.ResultRow{}
	if pengine.err != nil {
		return o, pengine.err
	}
	first := true
	nctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := range pengine.Execute(nctx) {
		if first {
			o = i
		}
		first = false
		cancel()
	}
	return o, nil
}
