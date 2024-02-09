package gdbi

import (
	"errors"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/copy"
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
func (t *BaseTraveler) AddCurrent(r DataRef) Traveler {
	o := BaseTraveler{
		Marks:  map[string]*DataElement{},
		Path:   make([]DataElementID, len(t.Path)+1),
		Signal: t.Signal,
	}
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	for i := range t.Path {
		o.Path[i] = t.Path[i]
	}
	if r != nil {
		rd := r.Get()
		if rd == nil {
			o.Path[len(t.Path)] = DataElementID{}
		} else if rd.To != "" {
			o.Path[len(t.Path)] = DataElementID{Edge: rd.ID}
		} else {
			o.Path[len(t.Path)] = DataElementID{Vertex: rd.ID}
		}
		o.Current = r.Get()
	}
	return &o
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *BaseTraveler) Copy() Traveler {
	o := BaseTraveler{
		Marks:  map[string]*DataElement{},
		Path:   make([]DataElementID, len(t.Path)),
		Signal: t.Signal,
	}
	for k, v := range t.Marks {
		vg := v.Get()
		o.Marks[k] = &DataElement{
			ID:    vg.ID,
			Label: vg.Label,
			From:  vg.From, To: vg.To,
			Data:   copy.DeepCopy(vg.Data).(map[string]interface{}),
			Loaded: vg.Loaded,
		}
	}
	for i := range t.Path {
		o.Path[i] = t.Path[i]
	}
	o.Current = t.Current
	return &o
}

func (tr *BaseTraveler) GetSignal() Signal {
	if tr.Signal == nil {
		return Signal{}
	}
	return *tr.Signal
}

func (tr *BaseTraveler) IsSignal() bool {
	return tr.Signal != nil
}

func (tr *BaseTraveler) IsNull() bool {
	return tr.Current == nil
}

// HasMark checks to see if a results is stored in a travelers statemap
func (t *BaseTraveler) HasMark(label string) bool {
	_, ok := t.Marks[label]
	return ok
}

// ListMarks returns the list of marks in a travelers statemap
func (t *BaseTraveler) ListMarks() []string {
	marks := []string{}
	for k := range t.Marks {
		marks = append(marks, k)
	}
	return marks
}

// AddMark adds a result to travels state map using `label` as the name
func (t *BaseTraveler) AddMark(label string, r DataRef) Traveler {
	o := BaseTraveler{Marks: map[string]*DataElement{}, Path: make([]DataElementID, len(t.Path))}
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	o.Marks[label] = r.Get()
	for i := range t.Path {
		o.Path[i] = t.Path[i]
	}
	o.Current = t.Current
	return &o
}

// GetMark gets stored result in travels state using its label
func (t *BaseTraveler) GetMark(label string) DataRef {
	return t.Marks[label]
}

// GetCurrent get current result value attached to the traveler
func (t *BaseTraveler) GetCurrent() DataRef {
	return t.Current
}

func (t *BaseTraveler) GetCurrentID() string {
	return t.Current.Get().ID
}

func (t *BaseTraveler) GetCount() uint32 {
	return t.Count
}

func (t *BaseTraveler) GetSelections() map[string]DataRef {
	out := map[string]DataRef{}
	for k, v := range t.Selections {
		out[k] = v
	}
	return out
}

func (t *BaseTraveler) GetRender() interface{} {
	return t.Render
}

func (t *BaseTraveler) GetPath() []DataElementID {
	return t.Path
}

func (t BaseTraveler) GetAggregation() *Aggregate {
	return t.Aggregation
}

func NewElementFromVertex(v *gripql.Vertex) *Vertex {
	return &Vertex{
		ID:     v.Gid,
		Label:  v.Label,
		Data:   v.Data.AsMap(),
		Loaded: true,
	}
}

func NewElementFromEdge(e *gripql.Edge) *Edge {
	return &Edge{
		ID:     e.Gid,
		Label:  e.Label,
		To:     e.To,
		From:   e.From,
		Data:   e.Data.AsMap(),
		Loaded: true,
	}
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
	/*
		out := map[string]interface{}{
			"gid":   "",
			"label": "",
			"to":    "",
			"from":  "",
			"data":  map[string]interface{}{},
		}
	*/
	out := map[string]interface{}{}
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

// Validate returns an error if the vertex is invalid
func (vertex *Vertex) Validate() error {
	if vertex.ID == "" {
		return errors.New("'gid' cannot be blank")
	}
	if vertex.Label == "" {
		return errors.New("'label' cannot be blank")
	}
	for k := range vertex.Data {
		err := gripql.ValidateFieldName(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewGraphElement(g *gripql.GraphElement) *GraphElement {
	o := GraphElement{Graph: g.Graph}
	if g.Vertex != nil {
		o.Vertex = NewElementFromVertex(g.Vertex)
	}
	if g.Edge != nil {
		o.Edge = NewElementFromEdge(g.Edge)
	}
	return &o
}
