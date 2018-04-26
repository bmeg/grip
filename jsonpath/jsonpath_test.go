package jsonpath

import (
	"testing"

	"github.com/bmeg/arachne/gdbi"
	"github.com/stretchr/testify/assert"
)

func TestRender(t *testing.T) {

	// test structures
	traveler := &gdbi.Traveler{}
	traveler = traveler.AddCurrent(&gdbi.DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
			"c": true,
			"d": []interface{}{1, 2, 3},
		},
	})
	traveler = traveler.AddMark("testMark", &gdbi.DataElement{
		ID:    "vertex2",
		Label: "bar",
		Data: map[string]interface{}{
			"a": "world",
			"b": 2,
			"c": false,
			"d": []interface{}{4, 5, 6},
		},
	})

	expected := traveler.GetCurrent().Data["a"]
	result := Render("$.a", traveler)
	assert.Equal(t, expected, result)

	expected = []interface{}{
		traveler.GetCurrent().Data["a"],
		traveler.GetCurrent().Data["b"],
		traveler.GetCurrent().Data["c"],
		traveler.GetCurrent().Data["d"],
	}
	result = Render([]interface{}{"$.a", "$.b", "$.c", "$.d"}, traveler)
	assert.Equal(t, expected, result)

	expected = map[string]interface{}{
		"current.gid":   traveler.GetCurrent().ID,
		"current.label": traveler.GetCurrent().Label,
		"current.a":     traveler.GetCurrent().Data["a"],
		"current.b":     traveler.GetCurrent().Data["b"],
		"current.c":     traveler.GetCurrent().Data["c"],
		"current.d":     traveler.GetCurrent().Data["d"],
		"mark.gid":      traveler.GetMark("testMark").ID,
		"mark.label":    traveler.GetMark("testMark").Label,
		"mark.a":        traveler.GetMark("testMark").Data["a"],
		"mark.b":        traveler.GetMark("testMark").Data["b"],
		"mark.c":        traveler.GetMark("testMark").Data["c"],
		"mark.d":        traveler.GetMark("testMark").Data["d"],
		"mark.d[0]":     4,
	}
	result = Render(map[string]interface{}{
		"current.gid":   "$.gid",
		"current.label": "$.label",
		"current.a":     "$.a",
		"current.b":     "$.b",
		"current.c":     "$.c",
		"current.d":     "$.d",
		"mark.gid":      "$testMark.gid",
		"mark.label":    "$testMark.label",
		"mark.a":        "$testMark.a",
		"mark.b":        "$testMark.b",
		"mark.c":        "$testMark.c",
		"mark.d":        "$testMark.d",
		"mark.d[0]":     "$testMark.d[0]",
	}, traveler)
	assert.Equal(t, expected, result)
}
