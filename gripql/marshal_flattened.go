package gripql

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type MarshalFlatten struct {
	marshal   protojson.MarshalOptions
	unmarshal protojson.UnmarshalOptions
}

func NewFlattenMarshaler() *MarshalFlatten {
	return &MarshalFlatten{
		protojson.MarshalOptions{EmitUnpopulated: true},
		protojson.UnmarshalOptions{},
	}
}

func (mflat *MarshalFlatten) Marshal(d interface{}) ([]byte, error) {
	switch x := d.(type) {
	case *Vertex:
		out := x.Data.AsMap()
		out["_gid"] = x.Gid
		out["_label"] = x.Label
		return json.Marshal(out)
	case *Edge:
		out := x.Data.AsMap()
		out["_gid"] = x.Gid
		out["_label"] = x.Label
		out["_to"] = x.To
		out["_from"] = x.From
		return json.Marshal(out)
	case *QueryResult:
		if e := x.GetVertex(); e != nil {
			out := e.Data.AsMap()
			out["_gid"] = e.Gid
			out["_label"] = e.Label
			return json.Marshal(map[string]any{"vertex": out})
		} else if e := x.GetEdge(); e != nil {
			out := e.Data.AsMap()
			out["_gid"] = e.Gid
			out["_label"] = e.Label
			out["_to"] = e.To
			out["_from"] = e.From
			return json.Marshal(map[string]any{"edge": out})
		}
	}
	if x, ok := d.(proto.Message); ok {
		return mflat.marshal.Marshal(x)
	}
	return json.Marshal(d)
}

func (mflat *MarshalFlatten) Unmarshal(data []byte, v interface{}) error {
	if x, ok := v.(proto.Message); ok {
		return mflat.unmarshal.Unmarshal(data, x)
	}
	return json.Unmarshal(data, v)
}
