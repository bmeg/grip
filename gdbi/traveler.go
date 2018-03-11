package gdbi

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

// AddCurrent creates a new copy of the travel with new 'current' value
func (t Traveler) AddCurrent(r *DataElement) *Traveler {
	o := Traveler{marks: map[string]*DataElement{}}
	for k, v := range t.marks {
		o.marks[k] = v
	}
	o.current = r
	return &o
}

// HasMark checks to see if a results is stored in a travelers statemap
func (t Traveler) HasMark(label string) bool {
	_, ok := t.marks[label]
	return ok
}

// AddMark adds a result to travels state map using `label` as the name
func (t Traveler) AddMark(label string, r *DataElement) *Traveler {
	o := Traveler{marks: map[string]*DataElement{}}
	for k, v := range t.marks {
		o.marks[k] = v
	}
	o.marks[label] = r
	o.current = t.current
	return &o
}

// GetMark gets stored result in travels state using its label
func (t Traveler) GetMark(label string) *DataElement {
	lt := t.marks[label]
	return lt
}

// GetCurrent get current result value attached to the traveler
func (t Traveler) GetCurrent() *DataElement {
	return t.current
}

// ToVertex converts data element to vertex
func (elem DataElement) ToVertex() *aql.Vertex {
	return &aql.Vertex{
		Gid:   elem.ID,
		Label: elem.Label,
		Data:  protoutil.AsStruct(elem.Data),
	}
}

// ToEdge converts data element to edge
func (elem DataElement) ToEdge() *aql.Edge {
	return &aql.Edge{
		Gid:   elem.ID,
		From:  elem.From,
		To:    elem.To,
		Label: elem.Label,
		Data:  protoutil.AsStruct(elem.Data),
	}
}

// ToDict converts data element to generic map
func (elem DataElement) ToDict() map[string]interface{} {
	out := map[string]interface{}{}
	if elem.ID != "" {
		out["gid"] = elem.ID
	}
	if elem.Label != "" {
		out["label"] = elem.Label
	}
	if elem.To != "" {
		out["to"] = elem.To
	}
	if elem.From != "" {
		out["from"] = elem.From
	}
	out["data"] = elem.Data
	return out
}
