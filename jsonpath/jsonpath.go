package jsonpath

import (
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
	//NE Not Equal
	NE
	//GT
	//LT
	//IN
)

//CompareFields compares two ProtoBuf json structs using JSONPaths
func CompareFields(a, b *structpb.Struct, aPath, bPath string, op Operator) (bool, error) {
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
	if op == NE {
		return !reflect.DeepEqual(aRes, bRes), nil
	}

	return false, nil
}
