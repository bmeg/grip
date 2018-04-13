package test

import (
	"encoding/json"
	"log"
	"math/rand"
	"testing"

	"github.com/bmeg/arachne/kvgraph"
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
    "age" : 50
  }
},{
	"gid" : "vertex3",
  "label" : "Dog",
  "data" : {
    "firstName" : "Fido",
    "lastName" : "Ruff",
    "age" : 3
  }
}]
`

var personDocs = []string{"vertex1", "vertex2"}
var bobDocs = []string{"vertex1"}
var lastNames = []string{"Smith", "Ruff"}
var firstNames = []string{"Bob", "Jack", "Fido"}

func contains(c string, s []string) bool {
	for _, i := range s {
		if c == i {
			return true
		}
	}
	return false
}

func randomString(n int) string {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func setupIndex(name string) (*kvindex.KVIndex, error) {
	dbPath := "test.db." + randomString(6)
	kvi, err := kvgraph.NewKVInterface(name, dbPath)
	if err != nil {
		return nil, err
	}
	return kvindex.NewIndex(kvi), nil
}

func TestFieldListing(t *testing.T) {
	for _, gName := range []string{"badger", "bolt", "level", "rocks"} {
		idx, err := setupIndex(gName)
		if err != nil {
			t.Fatal(err)
		}

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
}

func TestLoadDoc(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	for _, gName := range []string{"badger", "bolt", "level", "rocks"} {
		idx, err := setupIndex(gName)
		if err != nil {
			t.Fatal(err)
		}

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
		if count != 2 {
			t.Errorf("Wrong return count %d != %d", count, 2)
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
}

func TestTermEnum(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	for _, gName := range []string{"badger", "bolt", "level", "rocks"} {
		idx, err := setupIndex(gName)
		if err != nil {
			t.Fatal(err)
		}

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
		if count != 2 {
			t.Errorf("Wrong return count %d != %d", count, 2)
		}

		count = 0
		for d := range idx.FieldTerms("v.data.firstName") {
			count++
			if !contains(d.(string), firstNames) {
				t.Errorf("Bad term return: %s", d)
			}
		}
		if count != 3 {
			t.Errorf("Wrong return count %d != %d", count, 3)
		}
	}
}

func TestTermCount(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	for _, gName := range []string{"badger", "bolt", "level", "rocks"} {
		idx, err := setupIndex(gName)
		if err != nil {
			t.Fatal(err)
		}

		newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
		for _, s := range newFields {
			idx.AddField(s)
		}
		for _, d := range data {
			idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
		}

		count := 0
		for d := range idx.FieldTermCounts("v.data.lastName") {
			count++
			if !contains(string(d.Value), lastNames) {
				t.Errorf("Bad term return: %s", d.Value)
			}
			if string(d.Value) == "Smith" {
				if d.Count != 2 {
					t.Errorf("Bad term count return: %d", d.Count)
				}
			}
		}
		if count != 2 {
			t.Errorf("Wrong return count %d != %d", count, 2)
		}
		log.Printf("Counting: %d", count)
		count = 0
		for d := range idx.FieldTermCounts("v.data.firstName") {
			count++
			if !contains(string(d.Value), firstNames) {
				t.Errorf("Bad term return: %s", d.Value)
			}
		}
		if count != 3 {
			t.Errorf("Wrong return count %d != %d", count, 3)
		}
	}
}

func TestDocDelete(t *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	for _, gName := range []string{"badger", "bolt", "level", "rocks"} {
		idx, err := setupIndex(gName)
		if err != nil {
			t.Fatal(err)
		}

		newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
		for _, s := range newFields {
			idx.AddField(s)
		}
		for _, d := range data {
			idx.AddDoc(d["gid"].(string), map[string]interface{}{"v": d})
		}

		idx.RemoveDoc("vertex1")

		for d := range idx.FieldTermCounts("v.data.firstName") {
			if string(d.Value) == "Bob" {
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
}
