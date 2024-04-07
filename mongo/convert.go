package mongo

import (
	"github.com/bmeg/grip/gdbi"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// PackVertex take a GRIP vertex and convert it to a mongo doc
func PackVertex(v *gdbi.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = v.Data
	}
	out := map[string]interface{}{}
	for k, v := range p {
		out[k] = v
	}
	out["_id"] = v.ID
	out["_label"] = v.Label
	return out
}

// PackEdge takes a GRIP edge and converts it to a mongo doc
func PackEdge(e *gdbi.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Data != nil {
		p = e.Data
	}
	out := map[string]interface{}{}
	for k, v := range p {
		out[k] = v
	}
	out["_id"] = e.ID
	out["_from"] = e.From
	out["_to"] = e.To
	out["_label"] = e.Label
	return out
}

// UnpackVertex takes a mongo doc and converts it into an gripql.Vertex
func UnpackVertex(i map[string]interface{}) *gdbi.Vertex {
	o := &gdbi.Vertex{}
	o.ID = i["_id"].(string)
	o.Label = i["_label"].(string)
	d := removePrimatives(i).(map[string]any)
	o.Data = map[string]any{}
	for k, v := range d {
		if k != "_id" && k != "_label" {
			o.Data[k] = v
		}
	}
	o.Loaded = true
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an gripql.Edge
func UnpackEdge(i map[string]interface{}) *gdbi.Edge {
	o := &gdbi.Edge{}
	id := i["_id"]
	o.ID = id.(string)
	o.Label = i["_label"].(string)
	o.From = i["_from"].(string)
	o.To = i["_to"].(string)
	o.Data = map[string]any{}
	d := removePrimatives(i).(map[string]any)
	for k, v := range d {
		if k != "_id" && k != "_label" && k != "_to" && k != "_from" {
			o.Data[k] = v
		}
	}
	o.Loaded = true
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
	if x, ok := i.(primitive.ObjectID); ok {
		return x.String()
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
