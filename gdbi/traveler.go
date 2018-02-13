package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
)

const (
	stateCurrent = "_"
)


// These consts mark the type of a Pipeline traveler chan
const (
	// StateCustom The Pipeline will be emitting custom data structures
	StateCustom = 0
	// StateVertexList The Pipeline will be emitting a list of vertices
	StateVertexList = 1
	// StateEdgeList The Pipeline will be emitting a list of edges
	StateEdgeList = 2
	// StateRawVertexList The Pipeline will be emitting a list of all vertices, if there is an index
	// based filter, you can use skip listening and use that
	StateRawVertexList = 3
	// StateRawEdgeList The Pipeline will be emitting a list of all edges, if there is an index
	// based filter, you can use skip listening and use that
	StateRawEdgeList = 4
	// StateBundleList the Pipeline will be emittign a list of bundles
	StateBundleList = 5
)

// Pipeline represents the output of a single pipeline chain
type Pipeline interface {
	//StartInput(chan Traveler) error
	Start(ctx context.Context) chan Traveler
	GetCurrentState() int
	GetValueStates() map[string]int
}

// Traveler represents one query element, tracking progress across the graph
type Traveler struct {
	State map[string]aql.QueryResult
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t Traveler) AddCurrent(r aql.QueryResult) Traveler {
	o := Traveler{State: map[string]aql.QueryResult{}}
	for k, v := range t.State {
		o.State[k] = v
	}
	o.State[stateCurrent] = r
	return o
}

// HasLabeled checks to see if a results is stored in a travelers statemap
func (t Traveler) HasLabeled(label string) bool {
	_, ok := t.State[label]
	return ok
}

// AddLabeled adds a result to travels state map using `label` as the name
func (t Traveler) AddLabeled(label string, r aql.QueryResult) Traveler {
	o := Traveler{State: map[string]aql.QueryResult{}}
	for k, v := range t.State {
		o.State[k] = v
	}
	o.State[label] = r
	return o
}

// GetLabeled gets stored result in travels state using its label
func (t Traveler) GetLabeled(label string) *aql.QueryResult {
	lt := t.State[label]
	return &lt
}

// GetCurrent get current result value attached to the traveler
func (t Traveler) GetCurrent() *aql.QueryResult {
	lt := t.State[stateCurrent]
	return &lt
}
