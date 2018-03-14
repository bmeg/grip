package kvindex

import (
	"encoding/json"
	"github.com/bmeg/arachne/badgerdb"
	//	"log"
	"os"
	"testing"
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

func setupIndex() *KVIndex {
	kv, _ := badgerdb.BadgerBuilder("test.db")
	idx := NewIndex(kv)
	return idx
}

func closeIndex() {
	os.RemoveAll("test.db")
}

func TestFieldListing(b *testing.T) {
	idx := setupIndex()

	newFields := []string{"label", "data.firstName", "data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	count := 0
	for _, field := range idx.ListFields() {
		if !contains(field, newFields) {
			b.Errorf("Bad field return: %s", field)
		}
		count++
	}
	if count != len(newFields) {
		b.Errorf("Wrong return count %d != %d", count, len(newFields))
	}

	closeIndex()
}

func TestLoadDoc(b *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	idx := setupIndex()
	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}

	for _, d := range data {
		idx.AddDocPrefix(d["gid"].(string), d, "v")
	}

	count := 0
	for d := range idx.GetTermMatch("v.label", "Person") {
		if !contains(d, personDocs) {
			b.Errorf("Bad doc return: %s", d)
		}
		count++
	}
	if count != 2 {
		b.Errorf("Wrong return count %d != %d", count, 2)
	}

	count = 0
	for d := range idx.GetTermMatch("v.data.firstName", "Bob") {
		if !contains(d, bobDocs) {
			b.Errorf("Bad doc return: %s", d)
		}
		count++
	}
	if count != 1 {
		b.Errorf("Wrong return count %d != %d", count, 1)
	}
	closeIndex()
}

func TestTermCount(b *testing.T) {
	data := []map[string]interface{}{}
	json.Unmarshal([]byte(docs), &data)

	idx := setupIndex()
	newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
	for _, s := range newFields {
		idx.AddField(s)
	}
	for _, d := range data {
		idx.AddDocPrefix(d["gid"].(string), d, "v")
	}

	closeIndex()
}
