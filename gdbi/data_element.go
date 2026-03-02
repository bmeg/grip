package gdbi

import (
	"errors"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/types/known/structpb"
)

func (elem *DataElement) materializeRawData() {
	if elem == nil || elem.RawJSON == "" || elem.Data != nil {
		return
	}
	out := map[string]any{}
	if err := sonic.UnmarshalString(elem.RawJSON, &out); err != nil {
		return
	}
	elem.Data = out
	// Decoded payloads are treated as immutable by default.
	elem.Mutable = false
	elem.ModeHint = RowModeMaterialized
}

// EnsureMutablePayload clones the payload map on first write when the row is marked immutable.
func (elem *DataElement) EnsureMutablePayload() {
	if elem == nil {
		return
	}
	elem.materializeRawData()
	if elem.Mutable {
		return
	}
	if elem.Data != nil {
		cloned := make(map[string]any, len(elem.Data))
		for k, v := range elem.Data {
			cloned[k] = v
		}
		elem.Data = cloned
	}
	elem.Mutable = true
}

// ToVertex converts data element to vertex
func (elem *DataElement) ToVertex() *gripql.Vertex {
	elem.materializeRawData()
	sValue, err := structpb.NewStruct(elem.Data)
	if err != nil {
		log.Errorf("ToVertex: %s For elem.Data: '%#v'\n", err, elem.Data)
	}
	return &gripql.Vertex{
		Id:    elem.ID,
		Label: elem.Label,
		Data:  sValue,
	}
}

// ToEdge converts data element to edge
func (elem *DataElement) ToEdge() *gripql.Edge {
	elem.materializeRawData()
	sValue, err := structpb.NewStruct(elem.Data)
	if err != nil {
		log.Errorf("ToEdge: %s For elem.Data: '%#v'\n", err, elem.Data)
	}
	return &gripql.Edge{
		Id:    elem.ID,
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
			"_id":   "",
			"_label": "",
			"_to":    "",
			"_from":  "",
			"*":  map[string]interface{}{},
		}
	*/
	out := map[string]interface{}{}
	if elem == nil {
		return out
	}
	elem.materializeRawData()
	for k, v := range elem.Data {
		out[k] = v
	}
	out["_id"] = elem.ID
	out["_label"] = elem.Label
	out["_to"] = elem.To
	out["_from"] = elem.From
	return out
}

func (elem *DataElement) FromDict(d map[string]any) {
	if elem.Data == nil {
		elem.Data = map[string]any{}
	}
	for k, v := range d {
		switch k {
		case "_to":
			if vStr, ok := v.(string); ok {
				elem.To = vStr
			}
		case "_from":
			if vStr, ok := v.(string); ok {
				elem.From = vStr
			}
		case "_id":
			if vStr, ok := v.(string); ok {
				elem.ID = vStr
			}
		case "_label":
			if vStr, ok := v.(string); ok {
				elem.Label = vStr
			}
		default:
			elem.Data[k] = v
		}
	}
	elem.Loaded = true
	elem.Mutable = true
	elem.ModeHint = RowModeMaterialized
}

// Validate returns an error if the vertex is invalid
func (vertex *Vertex) Validate() error {
	if vertex.ID == "" {
		return errors.New("'_id' cannot be blank")
	}
	if vertex.Label == "" {
		return errors.New("'_label' cannot be blank")
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

func NewElementFromVertex(v *gripql.Vertex) *Vertex {
	return &Vertex{
		ID:       v.Id,
		Label:    v.Label,
		Data:     v.Data.AsMap(),
		Loaded:   true,
		Mutable:  true,
		ModeHint: RowModeMaterialized,
	}
}

func NewElementFromEdge(e *gripql.Edge) *Edge {
	return &Edge{
		ID:       e.Id,
		Label:    e.Label,
		To:       e.To,
		From:     e.From,
		Data:     e.Data.AsMap(),
		Loaded:   true,
		Mutable:  true,
		ModeHint: RowModeMaterialized,
	}
}
