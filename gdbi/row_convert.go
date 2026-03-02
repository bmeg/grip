package gdbi

import (
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

type vertexConvertible interface {
	ToVertex() *gripql.Vertex
}

type edgeConvertible interface {
	ToEdge() *gripql.Edge
}

// RowToVertex is the output-boundary adapter from runtime row state to API vertex shape.
func RowToVertex(r Row) *gripql.Vertex {
	if r == nil {
		return nil
	}
	if v, ok := r.(vertexConvertible); ok {
		return v.ToVertex()
	}
	data, _ := structpb.NewStruct(r.GetPayload())
	return &gripql.Vertex{
		Id:    r.GetID(),
		Label: r.GetLabel(),
		Data:  data,
	}
}

// RowToEdge is the output-boundary adapter from runtime row state to API edge shape.
func RowToEdge(r Row) *gripql.Edge {
	if r == nil {
		return nil
	}
	if e, ok := r.(edgeConvertible); ok {
		return e.ToEdge()
	}
	data, _ := structpb.NewStruct(r.GetPayload())
	return &gripql.Edge{
		Id:    r.GetID(),
		From:  r.GetFrom(),
		To:    r.GetTo(),
		Label: r.GetLabel(),
		Data:  data,
	}
}
