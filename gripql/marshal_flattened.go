package gripql

import (
	"fmt"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

func (mflat *MarshalFlatten) Marshal(d any) ([]byte, error) {
	switch x := d.(type) {
	case *Vertex:
		out := x.Data.AsMap()
		out["_id"] = x.Id
		out["_label"] = x.Label
		return sonic.ConfigFastest.Marshal(out)
	case *Edge:
		out := x.Data.AsMap()
		out["_id"] = x.Id
		out["_label"] = x.Label
		out["_to"] = x.To
		out["_from"] = x.From
		return sonic.ConfigFastest.Marshal(d)
	case *QueryResult:
		if e := x.GetVertex(); e != nil {
			out := e.Data.AsMap()
			out["_id"] = e.Id
			out["_label"] = e.Label
			return sonic.ConfigFastest.Marshal(map[string]any{"vertex": out})
		} else if e := x.GetEdge(); e != nil {
			out := e.Data.AsMap()
			out["_id"] = e.Id
			out["_label"] = e.Label
			out["_to"] = e.To
			out["_from"] = e.From
			return sonic.ConfigFastest.Marshal(map[string]any{"edge": out})
		}
	}
	if x, ok := d.(proto.Message); ok {
		return mflat.marshal.Marshal(x)
	}
	return sonic.ConfigFastest.Marshal(d)
}

func (mflat *MarshalFlatten) Unmarshal(data []byte, v any) error {
	if x, ok := v.(proto.Message); ok {
		if y, ok := v.(*Vertex); ok {
			z := map[string]any{}
			err := sonic.ConfigFastest.Unmarshal(data, &z)
			if err != nil {
				return err
			}
			data := map[string]any{}
			for k, v := range z {
				if k == "_id" {
					if kStr, ok := v.(string); ok {
						y.Id = kStr
					}
				} else if k == "_label" {
					if kStr, ok := v.(string); ok {
						y.Label = kStr
					}
				} else {
					data[k] = v
				}
			}
			s, err := structpb.NewStruct(data)
			if err != nil {
				fmt.Printf("NewStruct error: %s", err)
			}
			if err == nil {
				y.Data = s
				return nil
			}
		} else if y, ok := v.(*Edge); ok {
			z := map[string]any{}
			err := sonic.ConfigFastest.Unmarshal(data, &z)
			if err != nil {
				return err
			}
			data := map[string]any{}
			for k, v := range z {
				if k == "_id" {
					if kStr, ok := v.(string); ok {
						y.Id = kStr
					}
				} else if k == "_label" {
					if kStr, ok := v.(string); ok {
						y.Label = kStr
					}
				} else if k == "_to" {
					if kStr, ok := v.(string); ok {
						y.To = kStr
					}
				} else if k == "_from" {
					if kStr, ok := v.(string); ok {
						y.From = kStr
					}
				} else {
					data[k] = v
				}
			}
			s, err := structpb.NewStruct(data)
			if err == nil {
				y.Data = s
				return nil
			}
		}
		return mflat.unmarshal.Unmarshal(data, x)
	}
	return sonic.ConfigFastest.Unmarshal(data, v)
}
