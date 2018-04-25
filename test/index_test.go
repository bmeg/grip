package test

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/bmeg/arachne/kvindex"
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

func contains(c string, s []string) bool {
	for _, i := range s {
		if c == i {
			return true
		}
	}
	return false
}

func TestFieldListing(t *testing.T) {
	resetKVInterface()
	idx := kvindex.NewIndex(kvdriver)

	newFields := []string{"label", "data.firstName", "data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	count := 0
	for _, field := range idx.ListFields() {
		if !contains(field, newFields) {
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
	for d := range idx.GetTermMatch("v.label", "Person") {
		if !contains(d, personDocs) {
			t.Errorf("Bad doc return: %s", d)
		}
		count++
	}
	if count != 3 {
		b.Errorf("Wrong return count %d != %d", count, 3)
	}

	count = 0
	for d := range idx.GetTermMatch("v.data.firstName", "Bob") {
		if !contains(d, bobDocs) {
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
		if !contains(d.(string), lastNames) {
			t.Errorf("Bad term return: %s", d)
		}
	}
	if count != 3 {
		b.Errorf("Wrong return count %d != %d", count, 3)
	}

	count = 0
	for d := range idx.FieldTerms("v.data.firstName") {
		count++
		if !contains(d.(string), firstNames) {
			t.Errorf("Bad term return: %s", d)
		}
	}
	if count != 4 {
		b.Errorf("Wrong return count %d != %d", count, 4)
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
		if !contains(d.String, lastNames) {
			b.Errorf("Bad term return: %s", d.String)
		}
		if d.String == "Smith" {
			if d.Count != 2 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}
	if count != 3 {
		b.Errorf("Wrong return count %d != %d", count, 3)
	}
	log.Printf("Counting: %d", count)
	count = 0
	for d := range idx.FieldTermCounts("v.data.firstName") {
		count++
		if !contains(d.String, firstNames) {
			b.Errorf("Bad term return: %s", d.String)
		}
	}
	if count != 4 {
		b.Errorf("Wrong return count %d != %d", count, 4)
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
		idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
	}

	idx.RemoveDoc("vertex1")

	for d := range idx.FieldTermCounts("v.data.firstName") {
		if d.String == "Bob" {
			if d.Count != 0 {
				t.Errorf("Bad term count return: %d", d.Count)
			}
		}
	}

	count := 0
	for range idx.GetTermMatch("v.data.firstName", "Bob") {
		count++
	}
	if count != 0 {
		t.Errorf("Wrong return count %d != %d", count, 0)
	}
}

func TestNumField(b *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	idx := setupIndex()
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
		log.Printf("Age: %s", d)
	}

}
