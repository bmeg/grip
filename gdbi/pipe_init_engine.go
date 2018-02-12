package gdbi

import (
  "fmt"
	"context"
  "time"
  "sync"
  "strings"
  "log"
  "github.com/bmeg/arachne/aql"
)

func stateCustom(i int) int {
	switch i {
	case StateEdgeList:
		return StateEdgeList
	case StateVertexList:
		return StateVertexList
	case StateRawEdgeList:
		return StateEdgeList
	case StateRawVertexList:
		return StateVertexList
	default:
		return StateCustom
	}
}

type timer interface {
	startTimer(label string)
	endTimer(label string)
}

func newPipeOut(t chan Traveler, state int, valueStates map[string]int) PipeOut {
	return PipeOut{Travelers: t, State: state, ValueStates: valueStates}
}

type graphPipe func(t timer, ctx context.Context) PipeOut

// PipeEngine allows the construction of a chain evaluation steps in a query pipeline
// and then will execute a complex query and filter on a graph database interface
type PipeEngine struct {
	name       string
	db         GraphDB
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

type propKey string

var propLoad propKey = "load"


// NewPipeEngine creates a new PipeEngine based on the provided DBI
func NewPipeEngine(db GraphDB) *PipeEngine {
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


// Execute runs the current Pipeline engine
func (pengine *PipeEngine) Execute(ctx context.Context) chan aql.ResultRow {
	if pengine.pipe == nil {
		return nil
	}
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
