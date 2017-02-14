package protoutil

import (
	"github.com/golang/protobuf/ptypes/struct"
	"log"
)

func StructSet(s *structpb.Struct, key string, value interface{}) {
	switch v := value.(type) {
	case string:
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_StringValue{v}}
	case int:
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case int64:
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case float64:
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case bool:
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_BoolValue{v}}
	case *structpb.Value:
		s.Fields[key] = v
	case map[string]interface{}:
		o := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for k, v := range v {
			StructSet(o, k, v)
		}
		s.Fields[key] = &structpb.Value{Kind: &structpb.Value_StructValue{o}}
	default:
		log.Printf("unknown: %T", value)
	}
}

func CopyToStructSub(s *structpb.Struct, keys []string, values map[string]interface{}) {
	for _, i := range keys {
		StructSet(s, i, values[i])
	}
}

func CopyToStruct(s *structpb.Struct, values map[string]interface{}) {
	for i := range values {
		StructSet(s, i, values[i])
	}
}

func CopyStructToStruct(dst *structpb.Struct, src *structpb.Struct) {
	for k, v := range src.Fields {
		StructSet(dst, k, v)
	}
}

func CopyStructToStructSub(dst *structpb.Struct, keys []string, src *structpb.Struct) {
	for _, k := range keys {
		StructSet(dst, k, src.Fields[k])
	}
}

func AsMap(src *structpb.Struct) map[string]interface{} {
	out := map[string]interface{}{}
	for k, f := range src.Fields {
		if v, ok := f.Kind.(*structpb.Value_StringValue); ok {
			out[k] = v.StringValue
		} else if v, ok := f.Kind.(*structpb.Value_NumberValue); ok {
			out[k] = v.NumberValue
		} else if v, ok := f.Kind.(*structpb.Value_StructValue); ok {
			out[k] = AsMap(v.StructValue)
		}
	}
	return out
}

func AsStruct(src map[string]interface{}) *structpb.Struct {
	out := structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range src {
		StructSet(&out, k, v)
	}
	return &out
}
