package protoutil

import (
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
