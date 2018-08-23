package jsonpath

import (
	"os"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/stretchr/testify/assert"
)

var traveler *gdbi.Traveler

func TestMain(m *testing.M) {
	// test traveler
	traveler = &gdbi.Traveler{}
	traveler = traveler.AddCurrent(&gdbi.DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
			"c": true,
			"d": []interface{}{1, 2, 3},
			"e": []map[string]string{
				{"nested": "field1"},
				{"nested": "field2"},
			},
			"f": nil,
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
	e := m.Run()
	os.Exit(e)
}

func TestGetNamespace(t *testing.T) {
	expected := "foo"
	result := GetNamespace("$foo.bar[1:3].baz")
	assert.Equal(t, expected, result)

	result = GetNamespace("foo.bar[1:3].baz")
	assert.NotEqual(t, expected, result)
}

func TestGetJSONPath(t *testing.T) {
	expected := "$.data.a"
	result := GetJSONPath("a")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = GetJSONPath("_data.a")
	assert.Equal(t, expected, result)

	expected = "$.data.e[1].nested"
	result = GetJSONPath("e[1].nested")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = GetJSONPath("$testMark.a")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = GetJSONPath("testMark.a")
	assert.NotEqual(t, expected, result)
}

func TestGetDoc(t *testing.T) {
	expected := traveler.GetMark("testMark").ToDict()
	result := GetDoc(traveler, "testMark")
	assert.Equal(t, expected, result)

	expected = traveler.GetMark("i-dont-exist").ToDict()
	result = GetDoc(traveler, "i-dont-exist")
	assert.Equal(t, expected, result)

	expected = traveler.GetCurrent().ToDict()
	result = GetDoc(traveler, Current)
	assert.Equal(t, expected, result)
}

func TestTravelerPathExists(t *testing.T) {
	assert.True(t, TravelerPathExists(traveler, "_gid"))
	assert.True(t, TravelerPathExists(traveler, "_label"))
	assert.True(t, TravelerPathExists(traveler, "a"))
	assert.True(t, TravelerPathExists(traveler, "_data.a"))
	assert.False(t, TravelerPathExists(traveler, "non-existent"))
	assert.False(t, TravelerPathExists(traveler, "_data.non-existent"))

	assert.True(t, TravelerPathExists(traveler, "$testMark._gid"))
	assert.True(t, TravelerPathExists(traveler, "$testMark._label"))
	assert.True(t, TravelerPathExists(traveler, "$testMark.a"))
	assert.True(t, TravelerPathExists(traveler, "$testMark._data.a"))
	assert.False(t, TravelerPathExists(traveler, "$testMark.non-existent"))
	assert.False(t, TravelerPathExists(traveler, "$testMark._data.non-existent"))
}

func TestRender(t *testing.T) {
	expected := traveler.GetCurrent().Data["a"]
	result := RenderTraveler(traveler, "a")
	assert.Equal(t, expected, result)

	expected = []interface{}{
		traveler.GetCurrent().Data["a"],
		traveler.GetCurrent().Data["b"],
		traveler.GetCurrent().Data["c"],
		traveler.GetCurrent().Data["d"],
	}
	result = RenderTraveler(traveler, []interface{}{"a", "b", "c", "d"})
	assert.Equal(t, expected, result)

	expected = map[string]interface{}{
		"current.gid":         traveler.GetCurrent().ID,
		"current.label":       traveler.GetCurrent().Label,
		"current.a":           traveler.GetCurrent().Data["a"],
		"current.b":           traveler.GetCurrent().Data["b"],
		"current.c":           traveler.GetCurrent().Data["c"],
		"current.d":           traveler.GetCurrent().Data["d"],
		"mark.gid":            traveler.GetMark("testMark").ID,
		"mark.label":          traveler.GetMark("testMark").Label,
		"mark.a":              traveler.GetMark("testMark").Data["a"],
		"mark.b":              traveler.GetMark("testMark").Data["b"],
		"mark.c":              traveler.GetMark("testMark").Data["c"],
		"mark.d":              traveler.GetMark("testMark").Data["d"],
		"mark.d[0]":           4,
		"current.e[0].nested": "field1",
		"current.e.nested":    []interface{}{"field1", "field2"},
		"current.f":           traveler.GetCurrent().Data["f"],
	}
	result = RenderTraveler(traveler, map[string]interface{}{
		"current.gid":         "_gid",
		"current.label":       "_label",
		"current.a":           "a",
		"current.b":           "b",
		"current.c":           "c",
		"current.d":           "_data.d",
		"mark.gid":            "$testMark._gid",
		"mark.label":          "$testMark._label",
		"mark.a":              "$testMark.a",
		"mark.b":              "$testMark.b",
		"mark.c":              "$testMark._data.c",
		"mark.d":              "$testMark.d",
		"mark.d[0]":           "$testMark.d[0]",
		"current.e[0].nested": "_data.e[0].nested",
		"current.e.nested":    "e.nested",
		"current.f":           "f",
	})
	assert.Equal(t, expected, result)
}

func TestSelectFields(t *testing.T) {
	expected := &gdbi.Traveler{}
	expected = expected.AddCurrent(&gdbi.DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
		},
	})
	expected = expected.AddMark("testMark", &gdbi.DataElement{
		Data: map[string]interface{}{
			"b": 2,
			"d": []interface{}{4, 5, 6},
		},
	})

	result, err := SelectTravelerFields(traveler, "_gid", "_label", "a", "_data.b", "$testMark.b", "$testMark._data.d")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, result)

	expected = &gdbi.Traveler{}
	expected = expected.AddCurrent(&gdbi.DataElement{
		Data: traveler.GetCurrent().Data,
	})

	result, err = SelectTravelerFields(traveler, "_data")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, result)
}
