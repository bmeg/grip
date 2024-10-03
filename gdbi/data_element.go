package gdbi

import (
	"errors"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

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
	for k, v := range elem.Data {
		out[k] = v
	}
	if elem.ID != "" {
		out["_gid"] = elem.ID
	}
	if elem.Label != "" {
		out["_label"] = elem.Label
	}
	if elem.To != "" {
		out["_to"] = elem.To
	}
	if elem.From != "" {
		out["_from"] = elem.From
	}
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
		case "_gid":
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
