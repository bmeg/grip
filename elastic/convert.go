package elastic

import (
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

// PackVertex take a AQL vertex and convert it to a mongo doc
func PackVertex(v *gripql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = v.Data.AsMap()
	}
	//fmt.Printf("proto:%s\nmap:%s\n", v.Data, p)
	return map[string]interface{}{
		"gid":   v.Gid,
		"label": v.Label,
		"data":  p,
	}
}

// PackEdge takes a AQL edge and converts it to a mongo doc
func PackEdge(e *gripql.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Data != nil {
		p = e.Data.AsMap()
	}
	return map[string]interface{}{
		"gid":   e.Gid,
		"from":  e.From,
		"to":    e.To,
		"label": e.Label,
		"data":  p,
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
