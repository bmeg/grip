package jsonpath

import (
	"github.com/bmeg/arachne/protoutil"
	"testing"
)

func TestCompare(t *testing.T) {

	//test structure
	a := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": true,
		"d": []interface{}{1, 2, 3},
	}
	// same as test structure, but decared separately (to avoid comparing pointers)
	b := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": true,
		"d": []interface{}{1, 2, 3},
	}
	// different data
	c := map[string]interface{}{
		"a": "world",
		"b": 2,
		"c": false,
		"d": []interface{}{4, 5, 6},
	}

	aStruct := protoutil.AsStruct(a)
	bStruct := protoutil.AsStruct(b)
	cStruct := protoutil.AsStruct(c)

	if same, err := CompareStructFields(aStruct, bStruct, "$.a", "$.a", EQ); !same || err != nil {
		t.Errorf("Fail $.a == $.a (%s)", err)
	}
	if same, err := CompareStructFields(aStruct, cStruct, "$.a", "$.a", EQ); same || err != nil {
		t.Errorf("Fail $.a != $.a (%s)", err)
	}

	if same, err := CompareStructFields(aStruct, bStruct, "$.b", "$.b", EQ); !same || err != nil {
		t.Errorf("Fail $.b == $.b (%s)", err)
	}
	if same, err := CompareStructFields(aStruct, cStruct, "$.b", "$.b", EQ); same || err != nil {
		t.Errorf("Fail $.b != $.b (%s)", err)
	}

	if same, err := CompareStructFields(aStruct, bStruct, "$.c", "$.c", EQ); !same || err != nil {
		t.Errorf("Fail $.c == $.c (%s)", err)
	}
	if same, err := CompareStructFields(aStruct, cStruct, "$.c", "$.c", EQ); same || err != nil {
		t.Errorf("Fail $.c != $.c (%s)", err)
	}

	if same, err := CompareStructFields(aStruct, bStruct, "$.d", "$.d", EQ); !same || err != nil {
		t.Errorf("Fail $.d == $.d (%s)", err)
	}
	if same, err := CompareStructFields(aStruct, cStruct, "$.d", "$.d", EQ); same || err != nil {
		t.Errorf("Fail $.d != $.d (%s)", err)
	}

}
