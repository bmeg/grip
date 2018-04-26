package gdbi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectFields(t *testing.T) {
	traveler := &Traveler{
		current: &DataElement{
			ID:    "vertex1",
			Label: "foo",
			Data: map[string]interface{}{
				"a": "hello",
				"b": 1,
				"c": true,
				"d": []interface{}{1, 2, 3},
			},
		},
		marks: map[string]*DataElement{
			"testMark": {
				ID:    "vertex2",
				Label: "bar",
				Data: map[string]interface{}{
					"a": "world",
					"b": 2,
					"c": false,
					"d": []interface{}{4, 5, 6},
				},
			},
		},
	}

	expected := &Traveler{
		current: &DataElement{
			ID: "vertex1",
			Data: map[string]interface{}{
				"a": "hello",
			},
		},
		marks: map[string]*DataElement{
			"testMark": {
				Data: map[string]interface{}{
					"b": 2,
					"d": []interface{}{4, 5, 6},
				},
			},
		},
	}
	result, err := traveler.SelectFields("$.gid", "$.a", "$testMark.b", "$testMark.d")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, expected, result)
}
