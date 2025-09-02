package mongo

import (
	"maps"

	"github.com/bmeg/grip/gdbi"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// PackVertex take a GRIP vertex and convert it to a mongo doc
func PackVertex(v *gdbi.Vertex) map[string]any {
	p := map[string]any{}
	if v.Data != nil {
		p = v.Data
	}
	out := map[string]any{}
	maps.Copy(out, p)

	out[FIELD_ID] = v.ID
	out[FIELD_LABEL] = v.Label
	return out
}

// PackEdge takes a GRIP edge and converts it to a mongo doc
func PackEdge(e *gdbi.Edge) map[string]any {
	p := map[string]any{}
	if e.Data != nil {
		p = e.Data
	}
	out := map[string]any{}
	maps.Copy(out, p)

	out[FIELD_ID] = e.ID
	out[FIELD_FROM] = e.From
	out[FIELD_TO] = e.To
	out[FIELD_LABEL] = e.Label
	return out
}

// UnpackVertex takes a mongo doc and converts it into an gripql.Vertex
func UnpackVertex(i map[string]any) *gdbi.Vertex {
	o := &gdbi.Vertex{}
	o.ID = i[FIELD_ID].(string)
	o.Label = i[FIELD_LABEL].(string)
	d := removePrimatives(i).(map[string]any)
	o.Data = map[string]any{}
	for k, v := range d {
		if k != FIELD_ID && k != FIELD_LABEL {
			o.Data[k] = v
		}
	}
	o.Loaded = true
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an gripql.Edge
func UnpackEdge(i map[string]any) *gdbi.Edge {
	o := &gdbi.Edge{}
	id := i[FIELD_ID]
	o.ID = id.(string)
	o.Label = i[FIELD_LABEL].(string)
	o.From = i[FIELD_FROM].(string)
	o.To = i[FIELD_TO].(string)
	o.Data = map[string]any{}
	d := removePrimatives(i).(map[string]any)
	for k, v := range d {
		if k != FIELD_ID && k != FIELD_LABEL && k != FIELD_TO && k != FIELD_FROM {
			o.Data[k] = v
		}
	}
	o.Loaded = true
	return o
}

// this is needed because protobuf doesn't recognize primitive.A
// may want to find another solution, rather then copying the
// entire data structure
func removePrimatives(i any) any {
	if x, ok := i.(primitive.A); ok {
		out := make([]any, len(x))
		for i := range x {
			out[i] = removePrimatives(x[i])
		}
		return out
	}
	if x, ok := i.(primitive.ObjectID); ok {
		return x.String()
	}
	if x, ok := i.(map[string]any); ok {
		out := make(map[string]any)
		for i := range x {
			out[i] = removePrimatives(x[i])
		}
		return out
	}
	return i
}
