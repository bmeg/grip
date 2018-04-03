package elastic

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

var fieldLabel = "label"
var fieldSrc = "from"
var fieldDst = "to"

// PackVertex take a AQL vertex and convert it to a mongo doc
func PackVertex(v *aql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = protoutil.AsMap(v.Data)
	}
	//fmt.Printf("proto:%s\nmap:%s\n", v.Data, p)
	return map[string]interface{}{
		"_id":   v.Gid,
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
	o := map[string]interface{}{
		fieldSrc: e.From,
		fieldDst: e.To,
		"label":  e.Label,
		"data":   p,
	}
	if e.Gid != "" {
		o["_id"] = e.Gid
	}
	return o
}

// UnpackVertex takes a mongo doc and converts it into an aql.Vertex
func UnpackVertex(i map[string]interface{}) *aql.Vertex {
	o := &aql.Vertex{}
	o.Gid = i["_id"].(string)
	o.Label = i["label"].(string)
	if p, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(p.(map[string]interface{}))
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an aql.Edge
func UnpackEdge(i map[string]interface{}) *aql.Edge {
	o := &aql.Edge{}
	id := i["_id"]
	o.Gid = id.(string)
	o.Label = i["label"].(string)
	o.From = i[fieldSrc].(string)
	o.To = i[fieldDst].(string)
	if d, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(d.(map[string]interface{}))
	} else {
		o.Data = protoutil.AsStruct(map[string]interface{}{})
	}
	return o
}
