package core

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gdbi"
)

func TestIndexStartOptimize(t *testing.T) {
	original := []gdbi.Processor{}
	expected := []gdbi.Processor{}
	optimized := indexStartOptimize(original)
	if reflect.DeepEqual(optimized, expected) {
		t.Log("actual:", optimized)
		t.Log("expected:", exected)
		t.Error("indexStartOptimize returned an unexpected result")
	}
}
