package jsonpath

import (
	"fmt"
	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/oliveagle/jsonpath"
	"reflect"
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
