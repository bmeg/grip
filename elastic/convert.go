package elastic

import (
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

// PackVertex take a gdbi vertex and convert it to a mongo doc
func PackVertex(v *gdbi.Vertex) map[string]interface{} {
	return map[string]interface{}{
		"gid":   v.ID,
		"label": v.Label,
		"data":  v.Data,
	}
}

// PackEdge takes a AQL edge and converts it to a mongo doc
func PackEdge(e *gdbi.Edge) map[string]interface{} {
	return map[string]interface{}{
		"gid":   e.ID,
		"from":  e.From,
		"to":    e.To,
		"label": e.Label,
		"data":  e.Data,
	}
}

// UnpackVertex takes a mongo doc and converts it into an gripql.Vertex
func UnpackVertex(i map[string]interface{}) *gripql.Vertex {
	o := &gripql.Vertex{}
	o.Gid = i["gid"].(string)
	o.Label = i["label"].(string)
	if p, ok := i["data"]; ok {
		o.Data, _ = structpb.NewStruct(p.(map[string]interface{}))
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an gripql.Edge
func UnpackEdge(i map[string]interface{}) *gripql.Edge {
	o := &gripql.Edge{}
	o.Gid = i["gid"].(string)
	o.Label = i["label"].(string)
	o.From = i["from"].(string)
	o.To = i["to"].(string)
	if d, ok := i["data"]; ok {
		o.Data, _ = structpb.NewStruct(d.(map[string]interface{}))
	}
	return o
}
