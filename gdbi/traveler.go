package gdbi

import (
	"github.com/bmeg/arachne/aql"
)

const (
	stateCurrent = "_"
)

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
