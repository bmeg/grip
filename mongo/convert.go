package mongo

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	"gopkg.in/mgo.v2/bson"
)

var FIELD_SRC string = "src"
var FIELD_DST string = "dst"

func PackVertex(v aql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Properties != nil {
		p = protoutil.AsMap(v.Properties)
	}
	fmt.Printf("proto:%s\nmap:%s\n", v.Properties, p)
	return map[string]interface{}{
		"_id":        v.Gid,
		"label":      v.Label,
		"properties": p,
	}
}

func PackEdge(e aql.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Properties != nil {
		p = protoutil.AsMap(e.Properties)
	}
	o := map[string]interface{}{
		FIELD_SRC:    e.Src,
		FIELD_DST:    e.Dst,
		"label":      e.Label,
		"properties": p,
	}
	if e.Gid != "" {
		o["_id"] = e.Gid
	}
	return o
}

func UnpackVertex(i map[string]interface{}) aql.Vertex {
	o := aql.Vertex{}
	o.Gid = i["_id"].(string)
	o.Label = i["label"].(string)
	o.Properties = protoutil.AsStruct(i["properties"].(map[string]interface{}))
	return o
}

func UnpackEdge(i map[string]interface{}) aql.Edge {
	o := aql.Edge{}
	id := i["_id"]
	if idb, ok := id.(bson.ObjectId); ok {
		o.Gid = idb.String()
	} else {
		o.Gid = id.(string)
	}
	o.Label = i["label"].(string)
	o.Src = i[FIELD_SRC].(string)
	o.Dst = i[FIELD_DST].(string)
	o.Properties = protoutil.AsStruct(i["properties"].(map[string]interface{}))
	return o
}
