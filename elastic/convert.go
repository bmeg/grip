package elastic

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

// PackVertex take a AQL vertex and convert it to a mongo doc
func PackVertex(v *aql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = protoutil.AsMap(v.Data)
	}
	//fmt.Printf("proto:%s\nmap:%s\n", v.Data, p)
	return map[string]interface{}{
		"gid":   v.Gid,
		"label": v.Label,
		"data":  p,
	}
}

// PackEdge takes a AQL edge and converts it to a mongo doc
func PackEdge(e *aql.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Data != nil {
		p = protoutil.AsMap(e.Data)
	}
	return map[string]interface{}{
		"gid":   e.Gid,
		"from":  e.From,
		"to":    e.To,
		"label": e.Label,
		"data":  p,
	}
}

// UnpackVertex takes a mongo doc and converts it into an aql.Vertex
func UnpackVertex(i map[string]interface{}) *aql.Vertex {
	o := &aql.Vertex{}
	o.Gid = i["gid"].(string)
	o.Label = i["label"].(string)
	if p, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(p.(map[string]interface{}))
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an aql.Edge
func UnpackEdge(i map[string]interface{}) *aql.Edge {
	o := &aql.Edge{}
	o.Gid = i["gid"].(string)
	o.Label = i["label"].(string)
	o.From = i["from"].(string)
	o.To = i["to"].(string)
	if d, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(d.(map[string]interface{}))
	}
	return o
}
