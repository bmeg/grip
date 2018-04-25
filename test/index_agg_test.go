package test

import (
	"encoding/json"
	"fmt"
	"log"
	//"math"
	"testing"
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
	idx := setupIndex()

	newFields := []string{"value"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	data := []map[string]interface{}{}
	json.Unmarshal([]byte(numDocs), &data)
	for i, d := range data {
		log.Printf("Adding: %s", d)
		idx.AddDoc(fmt.Sprintf("%d", i), d)
	}

	//idx.AddDoc("a", map[string]interface{}{"value": math.Inf(1)})
	//idx.AddDoc("b", map[string]interface{}{"value": math.Inf(-1)})

	log.Printf("Scanning")
	for d := range idx.FieldTerms("value") {
		log.Printf("%s", d)
	}

	log.Printf("Min %f", idx.FieldTermNumberMin("value"))
	log.Printf("Max %f", idx.FieldTermNumberMax("value"))

	if idx.FieldTermNumberMin("value") != -200.0 {
		t.Errorf("Incorrect Min")
	}
	if idx.FieldTermNumberMax("value") != 400.7 {
		t.Errorf("Incorrect Max")
	}

	closeIndex()
}

func TestFloatRange(t *testing.T) {
	idx := setupIndex()

	newFields := []string{"value"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	data := []map[string]interface{}{}
	json.Unmarshal([]byte(numDocs), &data)
	for i, d := range data {
		//log.Printf("Adding: %s", d)
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
			t.Errorf("Incorrect term count")
		}
		if d.Number == 3.14 && d.Count != 3 {
			t.Errorf("Incorrect term count")
		}
		//log.Printf("%#v", d)
	}

	closeIndex()
}
