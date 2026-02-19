package gdbi

import (
	"github.com/bmeg/grip/gdbi/tpath"
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
	o := *t // Copy struct values (Marks, Path, etc. pointers are shared)
	if r != nil {
		o.Current = r.Get()

		// Some transform processors emit a DataElement with only Data set.
		// Treat that as the same current element identity.
		if t.Current != nil && o.Current != nil && o.Current.ID == "" && o.Current.From == "" && o.Current.To == "" {
			o.Current.ID = t.Current.ID
			o.Current.From = t.Current.From
			o.Current.To = t.Current.To
			if o.Current.Label == "" {
				o.Current.Label = t.Current.Label
			}
		}

		// Preserve existing path when current element identity does not change.
		if t.Current != nil && o.Current != nil &&
			t.Current.ID == o.Current.ID &&
			t.Current.From == o.Current.From &&
			t.Current.To == o.Current.To {
			if t.Path != nil {
				o.Path = make([]DataElementID, len(t.Path))
				copy(o.Path, t.Path)
			}
			return &o
		}

		// Bootstrap path tracking at the first traversal hop and append on each move.
		pathLen := len(t.Path)
		o.Path = make([]DataElementID, pathLen+1)
		copy(o.Path, t.Path)
		rd := o.Current
		if rd == nil {
			o.Path[pathLen] = DataElementID{}
		} else if rd.To != "" {
			o.Path[pathLen] = DataElementID{Edge: rd.ID}
		} else {
			o.Path[pathLen] = DataElementID{Vertex: rd.ID}
		}
	}
	return &o
}

// Copy creates a new copy of the traveler
func (t *BaseTraveler) Copy() Traveler {
	o := *t
	if len(t.Marks) > 0 {
		o.Marks = make(map[string]*DataElement, len(t.Marks))
		for k, v := range t.Marks {
			o.Marks[k] = v // Shallow copy of DataElement is fine as they are usually immutable
		}
	}
	if len(t.Path) > 0 {
		o.Path = make([]DataElementID, len(t.Path))
		copy(o.Path, t.Path)
	}
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
	if t.Marks == nil {
		return false
	}
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
	o := *t
	o.Marks = make(map[string]*DataElement, len(t.Marks)+1)
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	o.Marks[label] = r.Get()
	return &o
}

func (t *BaseTraveler) UpdateMark(label string, r DataRef) {
	if label == tpath.CURRENT {
		t.Current = r.Get()
		return
	}
	if t.Marks == nil {
		t.Marks = map[string]*DataElement{}
	}
	t.Marks[label] = r.Get()
}

// GetMark gets stored result in travels state using its label
func (t *BaseTraveler) GetMark(label string) DataRef {
	if t.Marks == nil {
		return nil
	}
	return t.Marks[label]
}

// GetCurrent get current result value attached to the traveler
func (t *BaseTraveler) GetCurrent() DataRef {
	return t.Current
}

func (t *BaseTraveler) GetCurrentID() string {
	if t.Current == nil {
		return ""
	}
	return t.Current.ID
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
