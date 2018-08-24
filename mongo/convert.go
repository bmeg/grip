package mongo

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/globalsign/mgo/bson"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

// PackVertex take a GRIP vertex and convert it to a mongo doc
func PackVertex(v *gripql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = protoutil.AsMap(v.Data)
	}
	return map[string]interface{}{
		"_id":   v.Gid,
		"label": v.Label,
		"data":  p,
	}
}

// PackEdge takes a GRIP edge and converts it to a mongo doc
func PackEdge(e *gripql.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Data != nil {
		p = protoutil.AsMap(e.Data)
	}
	return map[string]interface{}{
		"_id":   e.Gid,
		"from":  e.From,
		"to":    e.To,
		"label": e.Label,
		"data":  p,
	}
}

type pair struct {
	key         string
	valueMap    interface{}
	valueStruct *structpb.Struct
}

// UnpackVertex takes a mongo doc and converts it into an gripql.Vertex
func UnpackVertex(i map[string]interface{}) *gripql.Vertex {
	o := &gripql.Vertex{}
	o.Gid = i["_id"].(string)
	o.Label = i["label"].(string)
	if p, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(p.(map[string]interface{}))
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an gripql.Edge
func UnpackEdge(i map[string]interface{}) *gripql.Edge {
	o := &gripql.Edge{}
	id := i["_id"]
	if idb, ok := id.(bson.ObjectId); ok {
		o.Gid = idb.Hex()
	} else {
		o.Gid = id.(string)
	}
	o.Label = i["label"].(string)
	o.From = i["from"].(string)
	o.To = i["to"].(string)
	if d, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(d.(map[string]interface{}))
	} else {
		o.Data = protoutil.AsStruct(map[string]interface{}{})
	}
	return o
}
