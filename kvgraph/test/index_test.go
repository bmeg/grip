package test

import (
	"encoding/json"
	"testing"

	"context"

	"github.com/bmeg/grip/kvindex"
)

var docs = `[
{
  "gid" : "vertex1",
  "label" : "Person",
  "data" : {
    "firstName" : "Bob",
    "lastName" : "Smith",
    "age" : 35
  }
},{
  "gid" : "vertex2",
  "label" : "Person",
  "data" : {
    "firstName" : "Jack",
    "lastName" : "Smith",
    "age" : 50.2
  }
},{
  "gid" : "vertex3",
  "label" : "Person",
  "data" : {
    "firstName" : "Jill",
    "lastName" : "Jones",
    "age" : 35.1
  }
},{
	"gid" : "vertex4",
  "label" : "Dog",
  "data" : {
    "firstName" : "Fido",
    "lastName" : "Ruff",
    "age" : 3
  }
}]
`

var personDocs = []string{"vertex1", "vertex2", "vertex3"}
var bobDocs = []string{"vertex1"}
var lastNames = []string{"Smith", "Ruff", "Jones"}
var firstNames = []string{"Bob", "Jack", "Jill", "Fido"}

func TestFieldListing(t *testing.T) {
	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"label", "data.firstName", "data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	count := 0
	for _, field := range idx.ListFields() {
		if !contains(newFields, field) {
			t.Errorf("Bad field return: %s", field)
		}
		count++
	}
	if count != len(newFields) {
		t.Errorf("Wrong return count %d != %d", count, len(newFields))
	}
}

func TestLoadDoc(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	for _, d := range data {
		idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
	}

	count := 0
	for d := range idx.GetTermMatch(context.Background(), "v.label", "Person") {
		if !contains(personDocs, d) {
			t.Errorf("Bad doc return: %s", d)
		}
		count++
	}
	if count != 3 {
		t.Errorf("Wrong return count %d != %d", count, 3)
	}

	count = 0
	for d := range idx.GetTermMatch(context.Background(), "v.data.firstName", "Bob") {
		if !contains(bobDocs, d) {
			t.Errorf("Bad doc return: %s", d)
		}
		count++
	}
	if count != 1 {
		t.Errorf("Wrong return count %d != %d", count, 1)
	}
}

func TestTermEnum(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}
	for _, d := range data {
		idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
	}

	count := 0
	for d := range idx.FieldTerms("v.data.lastName") {
		count++
		if !contains(lastNames, d.(string)) {
			t.Errorf("Bad term return: %s", d)
		}
	}
	if count != 3 {
		t.Errorf("Wrong return count %d != %d", count, 3)
	}

	count = 0
	for d := range idx.FieldTerms("v.data.firstName") {
		count++
		if !contains(firstNames, d.(string)) {
			t.Errorf("Bad term return: %s", d)
		}
	}
	if count != 4 {
		t.Errorf("Wrong return count %d != %d", count, 4)
	}
}

func TestTermCount(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}
	for _, d := range data {
		idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
	}

	count := 0
	for d := range idx.FieldStringTermCounts("v.data.lastName") {
		count++
		if !contains(lastNames, d.String) {
			t.Errorf("Bad term return: %s", d.String)
		}
		if d.String == "Smith" {
			if d.Count != 2 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}
	if count != 3 {
		t.Errorf("Wrong return count %d != %d", count, 3)
	}

	count = 0
	for d := range idx.FieldTermCounts("v.data.firstName") {
		count++
		if !contains(firstNames, d.String) {
			t.Errorf("Bad term return: %s", d.String)
		}
	}
	if count != 4 {
		t.Errorf("Wrong return count %d != %d", count, 4)
	}
}

func TestDocDelete(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}
	for _, d := range data {
		err := idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
		if err != nil {
			t.Fatal("add doc failed", err)
		}
	}

	count := 0
	for d := range idx.FieldStringTermCounts("v.data.lastName") {
		count++
		if !contains(lastNames, d.String) {
			t.Errorf("Bad term return: %s", d.String)
		}
		if d.String == "Smith" {
			if d.Count != 2 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}
	if count != 3 {
		t.Errorf("Wrong return count %d != %d", count, 3)
	}

	err := idx.RemoveDoc("vertex1")
	if err != nil {
		t.Fatal("remove doc failed", err)
	}

	count = 0
	for d := range idx.FieldStringTermCounts("v.data.lastName") {
		count++
		if !contains(lastNames, d.String) {
			t.Errorf("Bad term return: %s", d.String)
		}
		if d.String == "Smith" {
			if d.Count != 1 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}
	if count != 3 {
		t.Errorf("Wrong return count %d != %d", count, 3)
	}

	for d := range idx.FieldTermCounts("v.data.firstName") {
		if d.String == "Bob" {
			if d.Count != 0 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}

	count = 0
	for range idx.GetTermMatch(context.Background(), "v.data.firstName", "Bob") {
		count++
	}
	if count != 0 {
		t.Errorf("Wrong return count %d != %d", count, 0)
	}
}

func TestNumField(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)
	newFields := []string{"v.label", "v.data.age"}
	for _, s := range newFields {
		idx.AddField(s)
	}
	for _, d := range data {
		idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
	}
	count := 0
	for d := range idx.FieldTerms("v.data.age") {
		count++
		t.Logf("Age: %v", d)
	}
}
