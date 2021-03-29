package protoutil

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewListFromStrings(s []string) *structpb.ListValue {
	values := make([]*structpb.Value, len(s))
	for i := range s {
		values[i] = structpb.NewStringValue(s[i])
	}
	return &structpb.ListValue{Values: values}
}

func AsStringList(src *structpb.ListValue) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src.Values))
	for i := range src.Values {
		out[i] = src.Values[i].GetStringValue()
	}
	return out
}

func StructMarshal(v map[string]interface{}) ([]byte, error) {
	s, err := structpb.NewStruct(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(s)
}

func StructUnMarshal(b []byte) (map[string]interface{}, error) {
	s, _ := structpb.NewStruct(map[string]interface{}{})
	err := proto.Unmarshal(b, s)
	if err != nil {
		return nil, err
	}
	return s.AsMap(), nil
}
