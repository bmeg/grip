package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bmeg/grip/kvindex"
)

var numDocs = `[
{"value" : 1},
{"value" : 2},
{"value" : 3},
{"value" : -42},
{"value" : 3.14},
{"value" : 70.1},
{"value" : 400.7},
{"value" : 3.14},
{"value" : 0.2},
{"value" : -0.2},
{"value" : -42},
{"value" : -150},
{"value" : -200},
{"value" : -15},
{"value" : -42},
{"value" : 3.14}
]`

func TestFloatSorting(t *testing.T) {
	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"value"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	data := []map[string]interface{}{}
	json.Unmarshal([]byte(numDocs), &data)
	for i, d := range data {
		t.Logf("Adding: %v", d)
		idx.AddDoc(fmt.Sprintf("%d", i), d)
	}

	last := -10000.0
	t.Logf("Scanning")
	count := 0
	for d := range idx.FieldNumbers("value") {
		if d < last {
			t.Errorf("Incorrect field return order: %f < %f", d, last)
		}
		last = d
		count++
		t.Logf("Scan, %f", d)
	}
	if count != len(data) {
		t.Errorf("Incorrect number of values returned: %v != %v", count, len(data))
	}

	if v := idx.FieldTermNumberMin("value"); v != -200.0 {
		t.Errorf("Incorrect Min %f != %f", v, -200.0)
	}

	if v := idx.FieldTermNumberMax("value"); v != 400.7 {
		t.Errorf("Incorrect Max: %f != %f", v, 400.7)
	}
}

func TestFloatRange(t *testing.T) {
	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"value"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	data := []map[string]interface{}{}
	json.Unmarshal([]byte(numDocs), &data)
	for i, d := range data {
		t.Logf("Adding: %s", d)
		idx.AddDoc(fmt.Sprintf("%d", i), d)
	}

	for d := range idx.FieldTermNumberRange("value", 5, 100) {
		if d.Number < 5 || d.Number > 100 {
			t.Errorf("Out of Range Value: %f", d.Number)
		}
	}

	for d := range idx.FieldTermNumberRange("value", -100, 10) {
		if d.Number < -100 || d.Number > 10 {
			t.Errorf("Out of Range Value: %f", d.Number)
		}
		if d.Number == -42 && d.Count != 3 {
			t.Errorf("Incorrect term count: %+v", d)
		}
		if d.Number == 3.14 && d.Count != 3 {
			t.Errorf("Incorrect term count: %+v", d)
		}
	}

}
