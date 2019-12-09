package protoutil

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/bmeg/grip/log"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

//StructSet take value and add it to Struct s using key
func StructSet(s *structpb.Struct, key string, value interface{}) {
	vw := WrapValue(value)
	s.Fields[key] = vw
}

// WrapValue takes a value and turns it into a protobuf structpb Value
func WrapValue(value interface{}) *structpb.Value {
	if value == nil {
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {

	case reflect.String:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v.String()}}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v.Int())}}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v.Uint())}}

	case reflect.Float32, reflect.Float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: v.Float()}}

	case reflect.Bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v.Bool()}}

	case reflect.Array, reflect.Slice:
		o := make([]*structpb.Value, v.Len())
		for i := 0; i < v.Len(); i++ {
			wv := WrapValue(v.Index(i).Interface())
			o[i] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: o}}}

	case reflect.Map:
		keys := v.MapKeys()
		o := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for _, key := range keys {
			k := fmt.Sprintf("%v", key.Interface())
			wv := WrapValue(v.MapIndex(key).Interface())
			o.Fields[k] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: o}}

	case reflect.Ptr, reflect.Struct:
		switch val := value.(type) {
		case *structpb.Value:
			return val

		case *structpb.Struct:
			return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: val}}

		case time.Time:
			return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: val.String()}}

		default:
			log.Errorf("wrap unknown pointer data type: %T", value)
		}

	default:
		log.Errorf("wrap unknown data type: %T", value)
	}
	return nil
}

// UnWrapValue takes protobuf structpb Value and return a native go value
func UnWrapValue(value *structpb.Value) interface{} {
	switch value.GetKind().(type) {
	case *structpb.Value_StringValue:
		return value.GetStringValue()

	case *structpb.Value_NumberValue:
		return value.GetNumberValue()

	case *structpb.Value_StructValue:
		return AsMap(value.GetStructValue())

	case *structpb.Value_ListValue:
		out := make([]interface{}, len(value.GetListValue().Values))
		for i := range value.GetListValue().Values {
			out[i] = UnWrapValue(value.GetListValue().Values[i])
		}
		return out

	case *structpb.Value_BoolValue:
		return value.GetBoolValue()

	case *structpb.Value_NullValue:
		return nil

	default:
		log.Errorf("unwrap unknown data type: %T", value.GetKind())
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
		out[k] = UnWrapValue(f)
	}
	return out
}

// AsBytes takes a protobuf Struct and converts it into a byte array
func AsBytes(src *structpb.Struct) []byte {
	if src == nil {
		return []byte{}
	}
	b, err := json.Marshal(AsMap(src))
	if err != nil {
		panic(err)
	}
	return b
}

// AsJSONString takes a protobuf Struct and converts it into a JSON string
func AsJSONString(src *structpb.Struct) string {
	if src == nil {
		return ""
	}
	m := jsonpb.Marshaler{}
	s, err := m.MarshalToString(src)
	if err != nil {
		panic(err)
	}
	return s
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
	if src == nil {
		return nil
	}
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

func NewStruct() *structpb.Struct {
	return &structpb.Struct{}
}
