
package jsonpath


import (
  "reflect"
  structpb "github.com/golang/protobuf/ptypes/struct"
  "github.com/oliveagle/jsonpath"
  "github.com/bmeg/arachne/protoutil"
)


type Operator int

const (
  EQ Operator = iota
  NE
  //GT
  //LT
  //IN
)


func CompareFields(a, b *structpb.Struct, aPath, bPath string, op Operator ) (bool, error) {
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
    return reflect.DeepEqual(aRes,bRes), nil
  }
  if op == NE {
    return !reflect.DeepEqual(aRes,bRes), nil
  }

  return false, nil
}


