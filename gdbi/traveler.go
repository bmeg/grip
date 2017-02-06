package gdbi

import (
	"github.com/bmeg/arachne/ophion"
)

const (
	STATE_CURRENT = "_"
)

func (t Traveler) AddCurrent(r ophion.QueryResult) Traveler {
	o := Traveler{State: map[string]ophion.QueryResult{}}
	for k, v := range t.State {
		o.State[k] = v
	}
	o.State[STATE_CURRENT] = r
	return o
}

func (t Traveler) GetCurrent() *ophion.QueryResult {
	lt := t.State[STATE_CURRENT]
	return &lt
}
