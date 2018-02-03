package protoutil

import (
	structpb "github.com/golang/protobuf/ptypes/struct"
	"log"
)

//StructSet take value and add it to Struct s using key
func StructSet(s *structpb.Struct, key string, value interface{}) {
	vw := WrapValue(value)
	s.Fields[key] = vw
}

// WrapValue takes a value and turns it into a protobuf structpb Value
func WrapValue(value interface{}) *structpb.Value {
	switch v := value.(type) {
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v}}
	case int:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}
	case int64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v}}
	case *structpb.Value:
		return v
	case []interface{}:
		o := make([]*structpb.Value, len(v))
		for i, k := range v {
			wv := WrapValue(k)
			o[i] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: o}}}
	case []string:
		o := make([]*structpb.Value, len(v))
		for i, k := range v {
			wv := &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: k}}
			o[i] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: o}}}
	case map[string]interface{}:
		o := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for k, v := range v {
			wv := WrapValue(v)
			o.Fields[k] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: o}}
	case map[string]float64:
		o := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for k, v := range v {
			wv := WrapValue(v)
			o.Fields[k] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: o}}
	case nil:
		return nil
	default:
		log.Printf("unknown data type: %T", value)
	}
	return nil
}

// CopyToStructSub copies a subset of keys from a map to a protobuf struct
func CopyToStructSub(s *structpb.Struct, keys []string, values map[string]interface{}) {
	for _, i := range keys {
		StructSet(s, i, values[i])
	}
}

// CopyToStruct copies values from map into protobuf struct
func CopyToStruct(s *structpb.Struct, values map[string]interface{}) {
	for i := range values {
		StructSet(s, i, values[i])
	}
}

// CopyStructToStruct copy the contents of one protobuf struct to another
func CopyStructToStruct(dst *structpb.Struct, src *structpb.Struct) {
	for k, v := range src.Fields {
		StructSet(dst, k, v)
	}
}

// CopyStructToStructSub copy the contents of one protobuf struct to another,
// but only using a subset of the keys
func CopyStructToStructSub(dst *structpb.Struct, keys []string, src *structpb.Struct) {
	for _, k := range keys {
		StructSet(dst, k, src.Fields[k])
	}
}

// AsMap takes a protobuf Struct and converts it into a go map
func AsMap(src *structpb.Struct) map[string]interface{} {
	if src == nil {
		return nil
	}
	out := map[string]interface{}{}
	for k, f := range src.Fields {
		if f == nil {
			out[k] = nil
		} else if v, ok := f.Kind.(*structpb.Value_StringValue); ok {
			out[k] = v.StringValue
		} else if v, ok := f.Kind.(*structpb.Value_NumberValue); ok {
			out[k] = v.NumberValue
		} else if v, ok := f.Kind.(*structpb.Value_StructValue); ok {
			out[k] = AsMap(v.StructValue)
		} else if v, ok := f.Kind.(*structpb.Value_BoolValue); ok {
			out[k] = v.BoolValue
		}
	}
	return out
}

// AsStruct takes a go map and converts it into a protobuf Struct
func AsStruct(src map[string]interface{}) *structpb.Struct {
	out := structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range src {
		StructSet(&out, k, v)
	}
	return &out
}

// AsStringList takes a protobuf ListValue and converts it into a []string
func AsStringList(src *structpb.ListValue) []string {
	out := make([]string, len(src.Values))
	for i := range src.Values {
		out[i] = src.Values[i].GetStringValue()
	}
	return out
}

// AsListValue takes a go []string and converts it into a protobuf ListValue
func AsListValue(str []string) *structpb.ListValue {
	v := make([]*structpb.Value, len(str))
	for i := range str {
		v[i] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: str[i]}}
	}
	o := structpb.ListValue{Values: v}
	return &o
}
