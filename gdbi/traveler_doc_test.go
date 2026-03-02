package gdbi

import (
	"os"
	"testing"

	"github.com/bmeg/grip/gdbi/tpath"

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
			"e": []map[string]interface{}{
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
	result := tpath.GetNamespace("$foo.bar[1:3].baz")
	assert.Equal(t, expected, result)

	result = tpath.GetNamespace("foo.bar[1:3].baz")
	assert.NotEqual(t, expected, result)
}

func TestGetJSONPath(t *testing.T) {
	expected := "$_current.a"
	result := tpath.NormalizePath("a")
	assert.Equal(t, expected, result)

	expected = "$_current.a"
	result = tpath.NormalizePath("$.a")
	assert.Equal(t, expected, result)

	expected = "$_current.e[1].nested"
	result = tpath.NormalizePath("e[1].nested")
	assert.Equal(t, expected, result)

	expected = "$testMark.a"
	result = tpath.NormalizePath("$testMark.a")
	assert.Equal(t, expected, result)

	expected = "$_current.testMark.a"
	result = tpath.NormalizePath("testMark.a")
	assert.Equal(t, expected, result)
}

func TestGetMarkDoc(t *testing.T) {
	expected := traveler.GetMark("testMark").ToDict()
	result := TravelerGetMarkDoc(traveler, "testMark")
	assert.Equal(t, expected, result)

	expected = map[string]any{}
	result = TravelerGetMarkDoc(traveler, "i-dont-exist")
	assert.Equal(t, expected, result)

	expected = traveler.GetCurrent().ToDict()
	result = TravelerGetMarkDoc(traveler, tpath.CURRENT)
	assert.Equal(t, expected, result)
}

func TestTravelerPathExists(t *testing.T) {
	assert.Equal(t, traveler.GetCurrent().GetID(), TravelerPathLookup(traveler, "$._id"))
	assert.Equal(t, traveler.GetCurrent().GetLabel(), TravelerPathLookup(traveler, "$._label"))
	assert.Equal(t, traveler.GetCurrent().GetFrom(), TravelerPathLookup(traveler, "$._from"))
	assert.Equal(t, traveler.GetCurrent().GetTo(), TravelerPathLookup(traveler, "$._to"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["a"], TravelerPathLookup(traveler, "$.a"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["b"], TravelerPathLookup(traveler, "$.b"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["c"], TravelerPathLookup(traveler, "$.c"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["d"], TravelerPathLookup(traveler, "$.d"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["e"], TravelerPathLookup(traveler, "$.e"))
	assert.Equal(t, traveler.GetCurrent().ToDict()["f"], TravelerPathLookup(traveler, "$.f"))

	assert.Equal(t, traveler.GetMark("testMark").GetID(), TravelerPathLookup(traveler, "$testMark._id"))
	assert.Equal(t, traveler.GetMark("testMark").GetLabel(), TravelerPathLookup(traveler, "$testMark._label"))
	assert.Equal(t, traveler.GetMark("testMark").ToDict()["a"], TravelerPathLookup(traveler, "$testMark.a"))
	assert.Equal(t, traveler.GetMark("testMark").ToDict()["b"], TravelerPathLookup(traveler, "$testMark.b"))
	assert.Equal(t, traveler.GetMark("testMark").ToDict()["c"], TravelerPathLookup(traveler, "$testMark.c"))
	assert.Equal(t, traveler.GetMark("testMark").ToDict()["d"], TravelerPathLookup(traveler, "$testMark.d"))

	assert.True(t, TravelerPathExists(traveler, "_id"))
	assert.True(t, TravelerPathExists(traveler, "$_id"))
	assert.True(t, TravelerPathExists(traveler, "_label"))
	assert.True(t, TravelerPathExists(traveler, "a"))
	assert.True(t, TravelerPathExists(traveler, "$a"))
	assert.True(t, TravelerPathExists(traveler, "$_current.a"))
	assert.False(t, TravelerPathExists(traveler, "non-existent"))
	assert.False(t, TravelerPathExists(traveler, "$_current.non-existent"))

	assert.True(t, TravelerPathExists(traveler, "$testMark._id"))
	assert.True(t, TravelerPathExists(traveler, "$testMark._label"))
	assert.True(t, TravelerPathExists(traveler, "$testMark.a"))
	assert.False(t, TravelerPathExists(traveler, "$testMark.non-existent"))
}

func TestRender(t *testing.T) {
	expected := traveler.GetCurrent().ToDict()["a"]
	result := RenderTraveler(traveler, "$.a")
	assert.Equal(t, expected, result)

	expected = []interface{}{
		traveler.GetCurrent().ToDict()["a"],
		traveler.GetCurrent().ToDict()["b"],
		traveler.GetCurrent().ToDict()["c"],
		traveler.GetCurrent().ToDict()["d"],
	}
	result = RenderTraveler(traveler, []interface{}{"a", "b", "c", "d"})
	assert.Equal(t, expected, result)

	expected = map[string]interface{}{
		"current.id":          traveler.GetCurrent().GetID(),
		"current.label":       traveler.GetCurrent().GetLabel(),
		"current.a":           traveler.GetCurrent().ToDict()["a"],
		"current.b":           traveler.GetCurrent().ToDict()["b"],
		"current.c":           traveler.GetCurrent().ToDict()["c"],
		"current.d":           traveler.GetCurrent().ToDict()["d"],
		"mark.id":             traveler.GetMark("testMark").GetID(),
		"mark.label":          traveler.GetMark("testMark").GetLabel(),
		"mark.a":              traveler.GetMark("testMark").ToDict()["a"],
		"mark.b":              traveler.GetMark("testMark").ToDict()["b"],
		"mark.c":              traveler.GetMark("testMark").ToDict()["c"],
		"mark.d":              traveler.GetMark("testMark").ToDict()["d"],
		"mark.d[0]":           4,
		"current.e[0].nested": "field1",
		"current.e.nested":    []interface{}{"field1", "field2"},
		"current.f":           traveler.GetCurrent().ToDict()["f"],
	}
	result = RenderTraveler(traveler, map[string]interface{}{
		"current.id":          "_id",
		"current.label":       "_label",
		"current.a":           "a",
		"current.b":           "b",
		"current.c":           "c",
		"current.d":           "d",
		"mark.id":             "$testMark._id",
		"mark.label":          "$testMark._label",
		"mark.a":              "$testMark.a",
		"mark.b":              "$testMark.b",
		"mark.c":              "$testMark.c",
		"mark.d":              "$testMark.d",
		"mark.d[0]":           "$testMark.d[0]",
		"current.e[0].nested": "e[0].nested",
		"current.e.nested":    "e.nested",
		"current.f":           "f",
	})
	assert.Equal(t, expected, result)
}

func TestIncludeFields(t *testing.T) {
	orig := map[string]any{
		"b": 1,
		"c": true,
		"e": []map[string]any{
			{"nested": "field1"},
			{"nested": "field2"},
		},
		"f": nil,
	}

	expected := map[string]any{
		"b": 1,
		"c": true,
	}
	result := includeFields(orig, []string{"b", "data.c"})
	assert.Equal(t, expected, result)

	result = includeFields(orig, []string{"b", "data.c", "doesnotexist", "data.idonotexist", "i.do.not.exist"})
	assert.Equal(t, expected, result)
}

func TestExcludeFields(t *testing.T) {
	orig := map[string]any{
		"b": 1,
		"c": true,
		"e": []map[string]any{
			{"nested": "field1"},
			{"nested": "field2"},
		},
		"f": nil,
	}
	expected := map[string]any{
		"b": 1,
		"c": true,
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
			"e": []map[string]interface{}{
				{"nested": "field1"},
				{"nested": "field2"},
			},
			"f": nil,
		},
		Loaded: true,
	})
	result := SelectTravelerFields(traveler, "-a", "-_data.d")
	assert.Equal(t, expected, result)

	expected = (&BaseTraveler{}).AddMark("testMark", traveler.GetMark("testMark"))
	expected = expected.AddCurrent(&DataElement{
		ID:     "vertex1",
		Label:  "foo",
		Data:   map[string]interface{}{},
		Loaded: true,
	})
	result = SelectTravelerFields(traveler)
	assert.Equal(t, expected, result)

	expected = (&BaseTraveler{}).AddMark("testMark", traveler.GetMark("testMark"))
	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
		},
		Loaded: true,
	})
	result = SelectTravelerFields(traveler, "a", "_data.b")
	assert.Equal(t, expected, result)

	result = SelectTravelerFields(traveler, "_id", "_label", "a", "_data.b")
	assert.Equal(t, expected, result)

	result = SelectTravelerFields(traveler, "_id", "_label", "a", "_data.b", "$testMark.b", "$testMark._data.d")
	assert.Equal(t, expected, result)

	expected = (&BaseTraveler{}).AddMark("testMark", traveler.GetMark("testMark"))
	expected = expected.AddCurrent(&DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"b": 1,
		},
		Loaded: true,
	})
	result = SelectTravelerFields(traveler, "-a", "b")
	assert.Equal(t, expected, result)
}
func TestTravelerPathLookup(t *testing.T) {
	de := &DataElement{
		ID:    "vertex1",
		Label: "foo",
		Data: map[string]interface{}{
			"a": "hello",
			"b": 1,
		},
		Loaded: true,
	}
	tr := (&BaseTraveler{}).AddCurrent(de)

	assert.Equal(t, "vertex1", TravelerPathLookup(tr, "_id"))
	assert.Equal(t, "foo", TravelerPathLookup(tr, "_label"))
	assert.Equal(t, "hello", TravelerPathLookup(tr, "a"))
	assert.Equal(t, 1, TravelerPathLookup(tr, "b"))
	assert.Nil(t, TravelerPathLookup(tr, "c"))
}

func TestTravelerPathLookupRaw(t *testing.T) {
	de := &DataElement{
		ID:      "vertex1",
		Label:   "foo",
		RawJSON: `{"a": "hello", "b": 1}`,
		Loaded:  false,
	}
	tr := (&BaseTraveler{}).AddCurrent(de)

	assert.Equal(t, "vertex1", TravelerPathLookup(tr, "_id"))
	assert.Equal(t, "foo", TravelerPathLookup(tr, "_label"))
	assert.Equal(t, "hello", TravelerPathLookup(tr, "a"))
	assert.Equal(t, 1.0, TravelerPathLookup(tr, "b")) // sonic unmarshals numbers to float64
	assert.Nil(t, TravelerPathLookup(tr, "c"))
}

func TestTravelerPathLookupWildcard(t *testing.T) {
	assert.Equal(t, []any{"field1", "field2"}, TravelerPathLookup(traveler, "e[*].nested"))
	assert.Equal(t, []any{"field1", "field2"}, TravelerPathLookup(traveler, "e.*.nested"))
}

func TestTravelerPathLookupWildcardRaw(t *testing.T) {
	de := &DataElement{
		ID:      "vertex1",
		Label:   "foo",
		RawJSON: `{"items":[{"score":1},{"score":3}]}`,
		Loaded:  false,
	}
	tr := (&BaseTraveler{}).AddCurrent(de)

	assert.Equal(t, []any{1.0, 3.0}, TravelerPathLookup(tr, "items[*].score"))
}

func TestTravelerPathLookupNestedWildcardRaw(t *testing.T) {
	de := &DataElement{
		ID:    "vertex1",
		Label: "Observation",
		RawJSON: `{
			"component":[
				{"code":{"coding":[{"code":"File_Format"}]}},
				{"code":{"coding":[{"code":"Atlas_Name"}]}}
			]
		}`,
		Loaded: false,
	}
	tr := (&BaseTraveler{}).AddCurrent(de)

	assert.Equal(t, []any{"File_Format", "Atlas_Name"}, TravelerPathLookup(tr, "component[*].code.coding[*].code"))
}

func TestTravelerDocHelpersNilCurrent(t *testing.T) {
	tr := &BaseTraveler{}

	assert.Equal(t, map[string]any{}, TravelerGetMarkDoc(tr, tpath.CURRENT))
	assert.Equal(t, map[string]any{tpath.CURRENT: map[string]any{}}, TravelerGetDoc(tr))
	assert.False(t, TravelerPathExists(tr, "$.a"))
	assert.Nil(t, TravelerPathLookup(tr, "$.a"))
}

func TestRenderTravelerNilCurrentNoPanic(t *testing.T) {
	tr := &BaseTraveler{}
	result := RenderTraveler(tr, []any{"$.eye_colors[*]", "$.a", "literal"})
	assert.Equal(t, []any{nil, nil, "literal"}, result)
}
