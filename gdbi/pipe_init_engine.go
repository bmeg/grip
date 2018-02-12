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

type pipeStart func(ctx context.Context) chan Traveler
type pipeState func() int
type pipeValueStates func() map[string]int

type pipeImpl struct {
  start pipeStart
  state pipeState
  valueStates pipeValueStates
}

func (p *pipeImpl) GetCurrentState() int {
  return p.state()
}

func (p *pipeImpl) GetValueStates() map[string]int {
  return p.valueStates()
}

func (p *pipeImpl) Start(ctx context.Context) chan Traveler {
  return p.start(ctx)
}

func newPipeOut(a pipeStart, b pipeState, c pipeValueStates) Pipeline {
	return &pipeImpl{start: a, state: b, valueStates: c}
}

// PipeEngine allows the construction of a chain evaluation steps in a query pipeline
// and then will execute a complex query and filter on a graph database interface
type PipeEngine struct {
	name       string
	db         GraphDB
	pipe       Pipeline
	err        error
	selection  []string
	imports    []string
	parent     *PipeEngine
	startTime  map[string]time.Time
	timing     map[string]time.Duration
	timingLock sync.Mutex
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
		pipe:      nil,
		startTime: map[string]time.Time{},
		timing:    map[string]time.Duration{},
	}
}


func (pengine *PipeEngine) append(name string, pipe Pipeline) *PipeEngine {
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

/*
// Chain runs a sub pipeline, that takes and from another pipeline
func (pengine *PipeEngine) Chain(ctx context.Context, input Pipeline) Pipeline {

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
*/

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
		pipe := pengine.pipe.Start(context.WithValue(ctx, propLoad, true))
		for i := range pipe {
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
