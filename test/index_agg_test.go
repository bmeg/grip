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
{"value" : 70.1},
{"value" : 400.7},
{"value" : 3.14},
{"value" : 0.2},
{"value" : -0.2},
{"value" : -150},
{"value" : -200},
{"value" : -15}
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
