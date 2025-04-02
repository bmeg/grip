package gdbi

import (
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/util/copy"
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

func (t *BaseTraveler) UpdateMark(label string, r DataRef) {
	if label == tpath.CURRENT {
		t.Current = r.Get()
		return
	}
	t.Marks[label] = r.Get()
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
