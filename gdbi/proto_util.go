package gdbi

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
