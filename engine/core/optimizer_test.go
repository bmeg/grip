package core

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/davecgh/go-spew/spew"
)

func TestIndexStartOptimize(t *testing.T) {
	expected := []gdbi.Processor{
		&LookupVerts{ids: []string{"1", "2", "3"}},
		&LookupVertexAdjOut{},
	}
	original := []gdbi.Processor{
		&LookupVerts{},
		&HasID{ids: []string{"1", "2", "3"}},
		&LookupVertexAdjOut{},
	}
	optimized := indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []gdbi.Processor{
		&LookupVerts{ids: []string{"1", "2", "3"}},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&Has{stmt: gripql.Within("_gid", "1", "2", "3")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// order shouldnt matter
	expected = []gdbi.Processor{
		&LookupVerts{ids: []string{"1", "2", "3"}},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&Has{stmt: gripql.Within("_gid", "1", "2", "3")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// only use the first statement
	expected = []gdbi.Processor{
		&LookupVerts{ids: []string{"1", "2", "3"}},
		&Has{stmt: gripql.Within("_gid", "4", "5")},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&HasID{ids: []string{"1", "2", "3"}},
		&Has{stmt: gripql.Within("_gid", "4", "5")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []gdbi.Processor{
		&LookupVertsIndex{labels: []string{"foo", "bar"}},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&HasLabel{labels: []string{"foo", "bar"}},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []gdbi.Processor{
		&LookupVertsIndex{labels: []string{"foo", "bar"}},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&Has{stmt: gripql.Within("_label", "foo", "bar")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []gdbi.Processor{
		&LookupVertsIndex{labels: []string{"foo", "bar"}},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&Has{stmt: gripql.Within("_label", "foo", "bar")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []gdbi.Processor{
		&LookupVertsIndex{labels: []string{"foo", "bar"}},
		&Has{stmt: gripql.Eq("_label", "baz")},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&HasLabel{labels: []string{"foo", "bar"}},
		&Has{stmt: gripql.Eq("_label", "baz")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// use gid over label to optimize queries
	expected = []gdbi.Processor{
		&LookupVerts{ids: []string{"1", "2", "3"}},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&Has{stmt: gripql.Within("_label", "foo", "bar")},
		&LookupVertexAdjOut{},
	}
	original = []gdbi.Processor{
		&LookupVerts{},
		&Has{stmt: gripql.Eq("$.data.foo", "bar")},
		&Has{stmt: gripql.Within("_label", "foo", "bar")},
		&Has{stmt: gripql.Within("_gid", "1", "2", "3")},
		&LookupVertexAdjOut{},
	}
	optimized = indexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

}
