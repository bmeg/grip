package gdbi

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
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
)

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *Traveler) AddCurrent(r *DataElement) *Traveler {
	o := Traveler{Marks: map[string]*DataElement{}}
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	o.Current = r
	return &o
}

// HasMark checks to see if a results is stored in a travelers statemap
func (t *Traveler) HasMark(label string) bool {
	_, ok := t.Marks[label]
	return ok
}

// ListMarks returns the list of marks in a travelers statemap
func (t *Traveler) ListMarks() []string {
	marks := []string{}
	for k := range t.Marks {
		marks = append(marks, k)
	}
	return marks
}

// AddMark adds a result to travels state map using `label` as the name
func (t *Traveler) AddMark(label string, r *DataElement) *Traveler {
	o := Traveler{Marks: map[string]*DataElement{}}
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	o.Marks[label] = r
	o.Current = t.Current
	return &o
}

// GetMark gets stored result in travels state using its label
func (t *Traveler) GetMark(label string) *DataElement {
	return t.Marks[label]
}

// GetCurrent get current result value attached to the traveler
func (t *Traveler) GetCurrent() *DataElement {
	return t.Current
}

// ToVertex converts data element to vertex
func (elem *DataElement) ToVertex() *gripql.Vertex {
	sValue, err := structpb.NewStruct(elem.Data)
	if err != nil {
		fmt.Printf("Error: %s %#v\n", err, elem.Data)
	}
	return &gripql.Vertex{
		Gid:   elem.ID,
		Label: elem.Label,
		Data:  sValue,
	}
}

// ToEdge converts data element to edge
func (elem *DataElement) ToEdge() *gripql.Edge {
	sValue, _ := structpb.NewStruct(elem.Data)
	return &gripql.Edge{
		Gid:   elem.ID,
		From:  elem.From,
		To:    elem.To,
		Label: elem.Label,
		Data:  sValue,
	}
}

// ToDict converts data element to generic map
func (elem *DataElement) ToDict() map[string]interface{} {
	out := map[string]interface{}{
		"gid":   "",
		"label": "",
		"to":    "",
		"from":  "",
		"data":  map[string]interface{}{},
	}
	if elem == nil {
		return out
	}
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
