package gdbi

import (
	"github.com/bmeg/arachne/aql"
)

const (
	STATE_CURRENT = "_"
)

func (t Traveler) AddCurrent(r aql.QueryResult) Traveler {
	o := Traveler{State: map[string]aql.QueryResult{}}
	for k, v := range t.State {
		o.State[k] = v
	}
	o.State[STATE_CURRENT] = r
	return o
}

func (t Traveler) AddLabeled(label string, r aql.QueryResult) Traveler {
	o := Traveler{State: map[string]aql.QueryResult{}}
	for k, v := range t.State {
		o.State[k] = v
	}
	o.State[label] = r
	return o
}

func (t Traveler) GetLabeled(label string) *aql.QueryResult {
	lt := t.State[label]
	return &lt
}

func (t Traveler) GetCurrent() *aql.QueryResult {
	lt := t.State[STATE_CURRENT]
	return &lt
}
