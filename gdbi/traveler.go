package gdbi

import (
	"github.com/bmeg/grip/gdbi/tpath"
)

func dataRefIdentity(r Row) Row {
	return r
}

type nilRow interface {
	IsNilRow() bool
}

func isNilRow(r Row) bool {
	if r == nil {
		return true
	}
	if nr, ok := r.(nilRow); ok {
		return nr.IsNilRow()
	}
	return false
}

func rowForTraveler(r Row) Row {
	if r == nil {
		return nil
	}
	// Immutable DataElements are safe to share across traveler copies.
	if de, ok := r.(*DataElement); ok && !de.Mutable {
		return de
	}
	return r.Copy()
}

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
func (t *BaseTraveler) AddCurrent(r Row) Traveler {
	o := *t // Copy struct values (Marks, Path, etc. pointers are shared)
	prev := t.Current
	if isNilRow(r) {
		o.Current = nil
		if (t.TrackPath || t.Path != nil) && t.Path != nil {
			o.Path = make([]DataElementID, len(t.Path))
			copy(o.Path, t.Path)
		}
		return &o
	}
	o.Current = rowForTraveler(r)

	// Some transform processors emit a DataElement with only Data set.
	// Treat that as the same current element identity.
	if prev != nil && o.Current != nil && o.Current.GetID() == "" && o.Current.GetFrom() == "" && o.Current.GetTo() == "" {
		// This part is tricky because Row is an interface.
		// If it's a *DataElement, we can update it.
		if de, ok := o.Current.(*DataElement); ok {
			de.ID = prev.GetID()
			de.From = prev.GetFrom()
			de.To = prev.GetTo()
			if de.Label == "" {
				de.Label = prev.GetLabel()
			}
		}
	}

	// Preserve existing path when current element identity does not change.
	trackPath := t.TrackPath || t.Path != nil
	if !trackPath {
		o.Path = nil
		return &o
	}

	if prev != nil && o.Current != nil &&
		prev.GetID() == o.Current.GetID() &&
		prev.GetFrom() == o.Current.GetFrom() &&
		prev.GetTo() == o.Current.GetTo() {
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
	} else if rd.GetTo() != "" {
		o.Path[pathLen] = DataElementID{Edge: rd.GetID()}
	} else {
		o.Path[pathLen] = DataElementID{Vertex: rd.GetID()}
	}
	return &o
}

// Copy creates a new copy of the traveler
func (t *BaseTraveler) Copy() Traveler {
	o := *t
	if t.Current != nil {
		o.Current = rowForTraveler(t.Current)
	}
	if len(t.Marks) > 0 {
		o.Marks = make(map[string]Row, len(t.Marks))
		for k, v := range t.Marks {
			o.Marks[k] = rowForTraveler(v)
		}
	}
	if len(t.Selections) > 0 {
		o.Selections = make(map[string]Row, len(t.Selections))
		for k, v := range t.Selections {
			o.Selections[k] = rowForTraveler(v)
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
func (t *BaseTraveler) AddMark(label string, r Row) Traveler {
	o := *t
	o.Marks = make(map[string]Row, len(t.Marks)+1)
	for k, v := range t.Marks {
		o.Marks[k] = v
	}
	if r != nil {
		o.Marks[label] = rowForTraveler(r)
	} else {
		o.Marks[label] = nil
	}
	return &o
}

func (t *BaseTraveler) UpdateMark(label string, r Row) {
	if label == tpath.CURRENT {
		if r != nil {
			t.Current = rowForTraveler(r)
		} else {
			t.Current = nil
		}
		return
	}
	// Copy on write for marks
	newMarks := make(map[string]Row, len(t.Marks)+1)
	for k, v := range t.Marks {
		newMarks[k] = v
	}
	if r != nil {
		newMarks[label] = rowForTraveler(r)
	} else {
		newMarks[label] = nil
	}
	t.Marks = newMarks
}

// GetMark gets stored result in travels state using its label
func (t *BaseTraveler) GetMark(label string) Row {
	if t.Marks == nil {
		return nil
	}
	return t.Marks[label]
}

// GetCurrent get current result value attached to the traveler
func (t *BaseTraveler) GetCurrent() Row {
	return t.Current
}

func (t *BaseTraveler) GetCurrentID() string {
	if t.Current == nil {
		return ""
	}
	return t.Current.GetID()
}

func (t *BaseTraveler) GetCount() uint32 {
	return t.Count
}

func (t *BaseTraveler) GetSelections() map[string]Row {
	out := map[string]Row{}
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
