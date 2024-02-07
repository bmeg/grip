package gdbi

import (
	"os"
	"testing"

	"github.com/bmeg/grip/travelerpath"
	"github.com/stretchr/testify/assert"
)

var traveler Traveler

func TestMain(m *testing.M) {
	// test traveler
	traveler = &BaseTraveler{}
	traveler = traveler.AddCurrent(&DataElement{
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
	traveler = traveler.AddMark("testMark", &DataElement{
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
	result := travelerpath.GetNamespace("$foo.bar[1:3].baz")
	assert.Equal(t, expected, result)

	result = travelerpath.GetNamespace("foo.bar[1:3].baz")
	assert.NotEqual(t, expected, result)
}

func TestGetJSONPath(t *testing.T) {
	expected := "$.data.a"
	result := travelerpath.GetJSONPath("a")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = travelerpath.GetJSONPath("_data.a")
	assert.Equal(t, expected, result)

	expected = "$.data.e[1].nested"
	result = travelerpath.GetJSONPath("e[1].nested")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = travelerpath.GetJSONPath("$testMark.a")
	assert.Equal(t, expected, result)

	expected = "$.data.a"
	result = travelerpath.GetJSONPath("testMark.a")
	assert.NotEqual(t, expected, result)
}

func TestGetDoc(t *testing.T) {
	expected := traveler.GetMark("testMark").Get().ToDict()
	result := GetDoc(traveler, "testMark")
	assert.Equal(t, expected, result)

	expected = traveler.GetMark("i-dont-exist").Get().ToDict()
	result = GetDoc(traveler, "i-dont-exist")
	assert.Equal(t, expected, result)

	expected = traveler.GetCurrent().Get().ToDict()
	result = GetDoc(traveler, travelerpath.Current)
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
	expected := traveler.GetCurrent().Get().Data["a"]
	result := RenderTraveler(traveler, "a")
	assert.Equal(t, expected, result)

	expected = []interface{}{
		traveler.GetCurrent().Get().Data["a"],
		traveler.GetCurrent().Get().Data["b"],
		traveler.GetCurrent().Get().Data["c"],
		traveler.GetCurrent().Get().Data["d"],
	}
	result = RenderTraveler(traveler, []interface{}{"a", "b", "c", "d"})
	assert.Equal(t, expected, result)

	expected = map[string]interface{}{
		"current.gid":         traveler.GetCurrent().Get().ID,
		"current.label":       traveler.GetCurrent().Get().Label,
		"current.a":           traveler.GetCurrent().Get().Data["a"],
		"current.b":           traveler.GetCurrent().Get().Data["b"],
		"current.c":           traveler.GetCurrent().Get().Data["c"],
		"current.d":           traveler.GetCurrent().Get().Data["d"],
		"mark.gid":            traveler.GetMark("testMark").Get().ID,
		"mark.label":          traveler.GetMark("testMark").Get().Label,
		"mark.a":              traveler.GetMark("testMark").Get().Data["a"],
		"mark.b":              traveler.GetMark("testMark").Get().Data["b"],
		"mark.c":              traveler.GetMark("testMark").Get().Data["c"],
		"mark.d":              traveler.GetMark("testMark").Get().Data["d"],
		"mark.d[0]":           4,
		"current.e[0].nested": "field1",
		"current.e.nested":    []interface{}{"field1", "field2"},
		"current.f":           traveler.GetCurrent().Get().Data["f"],
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

func TestIncludeFields(t *testing.T) {
	orig := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
			"c": true,
			"e": []map[string]string{
				{"nested": "field1"},
				{"nested": "field2"},
			},
			"f": nil,
		},
	}
	new := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data:  map[string]interface{}{},
	}

	expected := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
			"c": true,
		},
	}
	result := includeFields(new, orig, []string{"b", "data.c"})
	assert.Equal(t, expected, result)

	result = includeFields(new, orig, []string{"b", "data.c", "doesnotexist", "data.idonotexist", "i.do.not.exist"})
	assert.Equal(t, expected, result)
}

func TestExcludeFields(t *testing.T) {
	orig := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
			"c": true,
			"e": []map[string]string{
				{"nested": "field1"},
				{"nested": "field2"},
			},
			"f": nil,
		},
	}
	expected := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
			"c": true,
		},
	}

	result := excludeFields(orig, []string{"e", "data.f"})
	assert.Equal(t, expected, result)

	result = excludeFields(orig, []string{"e", "data.f", "doesnotexist", "data.idonotexist", "i.do.not.exist"})
	assert.Equal(t, expected, result)
}

func TestSelectFields(t *testing.T) {
	expected := (&BaseTraveler{}).AddMark("testMark", traveler.GetMark("testMark"))
	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
			"c": true,
			"e": []map[string]string{
				{"nested": "field1"},
				{"nested": "field2"},
			},
			"f": nil,
		},
	})
	result := SelectTravelerFields(traveler, "-a", "-_data.d")
	assert.Equal(t, expected, result)

	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data:  map[string]interface{}{},
	})
	result = SelectTravelerFields(traveler)
	assert.Equal(t, expected, result)

	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
		},
	})
	result = SelectTravelerFields(traveler, "a", "_data.b")
	assert.Equal(t, expected, result)

	result = SelectTravelerFields(traveler, "_gid", "_label", "a", "_data.b")
	assert.Equal(t, expected, result)

	result = SelectTravelerFields(traveler, "_gid", "_label", "a", "_data.b", "$testMark.b", "$testMark._data.d")
	assert.Equal(t, expected, result)

	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
		},
	})
	result = SelectTravelerFields(traveler, "-a", "b")
	assert.Equal(t, expected, result)
}
