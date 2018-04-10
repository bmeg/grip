package jsonpath

import (
	"fmt"
	"reflect"

	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/oliveagle/jsonpath"
)

// Operator the type of comparison operation to run
type Operator int

const (
	//EQ Equal
	EQ Operator = iota
	//NEQ Not Equal
	NEQ
	//GT
	//LT
	//IN
)

//CompareStructFields compares two ProtoBuf json structs using JSONPaths
func CompareStructFields(a, b *structpb.Struct, aPath, bPath string, op Operator) (bool, error) {
	aMap := protoutil.AsMap(a)
	bMap := protoutil.AsMap(b)

	aRes, err := jsonpath.JsonPathLookup(aMap, aPath)
	if err != nil {
		return false, err
	}
	bRes, err := jsonpath.JsonPathLookup(bMap, bPath)
	if err != nil {
		return false, err
	}
	if op == EQ {
		return reflect.DeepEqual(aRes, bRes), nil
	}
	if op == NEQ {
		return !reflect.DeepEqual(aRes, bRes), nil
	}
	return false, nil
}

//CompareFields compares two ProtoBuf json structs using JSONPaths
func CompareFields(aMap, bMap map[string]interface{}, aPath, bPath string, op Operator) (bool, error) {
	aRes, err := jsonpath.JsonPathLookup(aMap, aPath)
	if err != nil {
		return false, err
	}
	bRes, err := jsonpath.JsonPathLookup(bMap, bPath)
	if err != nil {
		return false, err
	}
	if op == EQ {
		return reflect.DeepEqual(aRes, bRes), nil
	}
	if op == NEQ {
		return !reflect.DeepEqual(aRes, bRes), nil
	}
	return false, nil
}

//GetString gets a string value of a field
func GetString(a map[string]interface{}, path string) string {
	res, err := jsonpath.JsonPathLookup(a, path)
	if err != nil {
		return ""
	}
	if x, ok := res.(string); ok {
		return x
	}
	return fmt.Sprintf("%#v", res)
}

// Render takes a template and fills in the values using the data structure
func Render(template interface{}, data map[string]interface{}) interface{} {
	switch elem := template.(type) {
	case string:
		return GetString(data, elem)
	case map[string]interface{}:
		o := make(map[string]interface{}, len(elem))
		for k, v := range elem {
			o[k] = Render(v, data)
		}
		return o
	case []interface{}:
		o := make([]interface{}, len(elem))
		for i := range elem {
			o[i] = Render(elem[i], data)
		}
		return o
	default:
		return nil
	}
}
