package grids

import (
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/util/copy"
)

type GRIDTraveler struct {
	Graph       *Graph
	Current     *GRIDDataElement
	Marks       map[string]*GRIDDataElement
	Selections  map[string]*GRIDDataElement
	Aggregation *gdbi.Aggregate
	Count       uint32
	Render      interface{}
	Path        []GRIDDataElementID
	Signal      *gdbi.Signal
}

type GRIDDataElementID struct {
	IsVertex bool
	Gid      uint64
}

type GRIDDataElement struct {
	Gid    uint64
	To     uint64
	From   uint64
	Label  uint64
	Data   map[string]interface{}
	Loaded bool
}

func (rd *GRIDDataElement) VertexDataElement(ggraph *Graph) *gdbi.Vertex {
	Gid, _ := ggraph.keyMap.GetVertexID(rd.Gid)
	Label, _ := ggraph.keyMap.GetLabelID(rd.Label)
	return &gdbi.DataElement{ID: Gid, Label: Label, Data: rd.Data, Loaded: rd.Loaded}
}

func (rd *GRIDDataElement) EdgeDataElement(ggraph *Graph) *gdbi.Edge {
	Gid, _ := ggraph.keyMap.GetEdgeID(rd.Gid)
	Label, _ := ggraph.keyMap.GetLabelID(rd.Label)
	To, _ := ggraph.keyMap.GetVertexID(rd.To)
	From, _ := ggraph.keyMap.GetVertexID(rd.From)
	return &gdbi.DataElement{ID: Gid, To: To, From: From, Label: Label, Data: rd.Data, Loaded: rd.Loaded}
}

func (rd *GRIDDataElement) DataElement(ggraph *Graph) *gdbi.DataElement {
	if rd.To != 0 {
		return rd.EdgeDataElement(ggraph)
	}
	return rd.VertexDataElement(ggraph)
}

func (rd *GRIDDataElement) Copy() *GRIDDataElement {
	return &GRIDDataElement{
		Gid:    rd.Gid,
		Label:  rd.Label,
		To:     rd.To,
		From:   rd.From,
		Loaded: rd.Loaded,
		Data:   copy.DeepCopy(rd.Data).(map[string]interface{}),
	}
}

func DataElementToGRID(d *gdbi.DataElement, g *Graph) (*GRIDDataElement, error) {
	if d.To != "" {
		Gid, _ := g.keyMap.GetEdgeKey(d.ID)
		Label, _ := g.keyMap.GetLabelKey(d.Label)
		To, _ := g.keyMap.GetVertexKey(d.To)
		From, _ := g.keyMap.GetVertexKey(d.From)
		return &GRIDDataElement{
			Gid: Gid, Label: Label, To: To, From: From, Data: d.Data, Loaded: d.Loaded,
		}, nil
	}
	Gid, _ := g.keyMap.GetVertexKey(d.ID)
	Label, _ := g.keyMap.GetLabelKey(d.Label)
	o := &GRIDDataElement{
		Gid: Gid, Label: Label, Data: d.Data, Loaded: d.Loaded,
	}
	return o, nil
}

func NewGRIDTraveler(tr gdbi.Traveler, isVertex bool, gg *Graph) *GRIDTraveler {
	if a, ok := tr.(*GRIDTraveler); ok {
		return a
	}

	if tr.IsSignal() {
		s := tr.GetSignal()
		return &GRIDTraveler{Signal: &s}
	}

	cur := tr.GetCurrent()
	el, _ := DataElementToGRID(cur, gg)
	o := &GRIDTraveler{
		Graph:       gg,
		Current:     el,
		Aggregation: tr.GetAggregation(),
		Count:       tr.GetCount(),
		Render:      tr.GetRender(),
		Path:        []GRIDDataElementID{},
		Marks:       map[string]*GRIDDataElement{},
	}
	for _, e := range tr.GetPath() {
		if e.Vertex != "" {
			l, _ := gg.keyMap.GetVertexKey(e.Vertex)
			o.Path = append(o.Path, GRIDDataElementID{IsVertex: true, Gid: l})
		} else {
			l, _ := gg.keyMap.GetVertexKey(e.Edge)
			o.Path = append(o.Path, GRIDDataElementID{IsVertex: false, Gid: l})
		}
	}
	for _, k := range tr.ListMarks() {
		m := tr.GetMark(k)
		o.Marks[k], _ = DataElementToGRID(m, gg)
	}

	return o
}

func (tr *GRIDTraveler) GetSignal() gdbi.Signal {
	if tr.Signal == nil {
		return gdbi.Signal{}
	}
	return *tr.Signal
}

func (tr *GRIDTraveler) IsSignal() bool {
	return tr.Signal != nil
}

func (tr *GRIDTraveler) IsNull() bool {
	return tr.Current != nil
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *GRIDTraveler) AddCurrent(r gdbi.DataRef) gdbi.Traveler {
	a := t.GRIDCopy()
	c, _ := DataElementToGRID(r, t.Graph)
	a.Current = c
	return a
}

func (t *GRIDTraveler) AddRawCurrent(r *GRIDDataElement) *GRIDTraveler {
	o := GRIDTraveler{
		Graph: t.Graph,
		Marks: map[string]*GRIDDataElement{},
		Path:  make([]GRIDDataElementID, len(t.GetPath())+1),
	}

	for k, v := range t.Marks {
		o.Marks[k] = v.Copy()
	}
	for i := range t.Path {
		o.Path[i] = t.Path[i]
	}
	if r == nil {
		o.Path[len(t.Path)] = GRIDDataElementID{}
	} else if r.To != 0 {
		o.Path[len(t.Path)] = GRIDDataElementID{Gid: r.Gid, IsVertex: false}
	} else {
		o.Path[len(t.Path)] = GRIDDataElementID{Gid: r.Gid, IsVertex: true}
	}
	o.Current = r
	return &o
}

func (t *GRIDTraveler) GetCurrentID() string {
	if t.Current.To == 0 {
		s, _ := t.Graph.keyMap.GetVertexID(t.Current.Gid)
		return s
	} else {
		s, _ := t.Graph.keyMap.GetEdgeID(t.Current.Gid)
		return s
	}
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *GRIDTraveler) Copy() gdbi.Traveler {
	return t.GRIDCopy()
}

func (t *GRIDTraveler) GRIDCopy() *GRIDTraveler {
	o := GRIDTraveler{
		Graph:       t.Graph,
		Marks:       map[string]*GRIDDataElement{},
		Path:        make([]GRIDDataElementID, len(t.GetPath())),
		Signal:      t.Signal,
		Count:       t.Count,
		Aggregation: t.Aggregation,
	}
	for k, v := range t.Marks {
		o.Marks[k] = v.Copy()
	}
	for i := range t.Path {
		o.Path[i] = t.Path[i]
	}
	o.Current = t.Current
	return &o
}

// HasMark checks to see if a results is stored in a travelers statemap
func (t *GRIDTraveler) HasMark(label string) bool {
	_, ok := t.Marks[label]
	return ok
}

// ListMarks returns the list of marks in a travelers statemap
func (t *GRIDTraveler) ListMarks() []string {
	marks := []string{}
	for k := range t.Marks {
		marks = append(marks, k)
	}
	return marks
}

// AddMark adds a result to travels state map using `label` as the name
func (t *GRIDTraveler) AddMark(label string, r gdbi.DataRef) gdbi.Traveler {
	o := t.GRIDCopy()
	n, _ := DataElementToGRID(r, t.Graph)
	o.Marks[label] = n
	return o
}

// GetMark gets stored result in travels state using its label
func (t *GRIDTraveler) GetMark(label string) *gdbi.DataElement {
	return t.Marks[label].DataElement(t.Graph)
}

// GetCurrent get current result value attached to the traveler
func (t *GRIDTraveler) GetCurrent() *gdbi.DataElement {
	return t.Current.DataElement(t.Graph)
}

func (t *GRIDTraveler) GetCount() uint32 {
	return t.Count
}

func (t *GRIDTraveler) GetSelections() map[string]*gdbi.DataElement {
	o := map[string]*gdbi.DataElement{}
	for k, v := range t.Selections {
		o[k] = v.DataElement(t.Graph)
	}
	return o
}

func (t *GRIDTraveler) GetRender() interface{} {
	return t.Render
}

func (t *GRIDTraveler) GetPath() []gdbi.DataElementID {
	out := make([]gdbi.DataElementID, len(t.Path))
	for i := range t.Path {
		e := t.Path[i]
		if e.IsVertex {
			s, _ := t.Graph.keyMap.GetVertexID(e.Gid)
			out[i] = gdbi.DataElementID{Vertex: s}
		} else {
			s, _ := t.Graph.keyMap.GetEdgeID(e.Gid)
			out[i] = gdbi.DataElementID{Edge: s}
		}
	}
	return out
}

func (t GRIDTraveler) GetAggregation() *gdbi.Aggregate {
	return t.Aggregation
}
