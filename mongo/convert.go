package mongo

import (
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// PackVertex take a GRIP vertex and convert it to a mongo doc
func PackVertex(v *gripql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = v.Data.AsMap()
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
		p = e.Data.AsMap()
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
	if d, ok := i["data"]; ok {
		d = removePrimatives(d)
		o.Data, _ = structpb.NewStruct( d.(map[string]interface{}) )
	} else {
		o.Data, _ = structpb.NewStruct( map[string]interface{}{} )
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an gripql.Edge
func UnpackEdge(i map[string]interface{}) *gripql.Edge {
	o := &gripql.Edge{}
	id := i["_id"]
	o.Gid = id.(string)
	o.Label = i["label"].(string)
	o.From = i["from"].(string)
	o.To = i["to"].(string)
	if d, ok := i["data"]; ok {
		o.Data, _ = structpb.NewStruct( d.(map[string]interface{}) )
	} else {
		o.Data, _ = structpb.NewStruct( map[string]interface{}{} )
	}
	return o
}

// this is needed because protobuf doesn't recognize primitive.A
// may want to find another solution, rather then copying the
// entire data structure
func removePrimatives(i interface{}) interface{} {
	if x, ok := i.(primitive.A); ok {
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = removePrimatives(x[i])
		}
		return out
	}
	if x, ok := i.(map[string]interface{}); ok {
		out := make(map[string]interface{})
		for i := range x {
			out[i] = removePrimatives(x[i])
		}
		return out
	}
	return i
}
