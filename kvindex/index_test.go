
package kvindex

import (
  "os"
  "log"
  "testing"
  "encoding/json"
  "github.com/bmeg/arachne/badgerdb"
)


var doc1 = `
{
  "gid" : "vertex1",
  "label" : "Person",
  "data" : {
    "firstName" : "Bob",
    "lastName" : "Smith",
    "age" : 35
  }
}
`

func setupIndex() *KVIndex {
  kv, _ := badgerdb.BadgerBuilder("test.db")
  idx := NewIndex(kv)
  return idx
}

func closeIndex() {
  os.RemoveAll("test.db")
}

func contains(c string, s []string) bool {
  for _, i := range s {
    if c == i { return true }
  }
  return false
}

func TestFieldListing(b *testing.T) {
  idx := setupIndex()

  newFields := []string{"label", "data.firstName", "data.lastName"}
  for _, s := range newFields {
    idx.AddField(s)
  }

  count := 0
  for field := range idx.ListFields() {
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
  data := map[string]interface{}{}
  json.Unmarshal([]byte(doc1), &data)
  log.Printf("%s", data)

  idx := setupIndex()
  newFields := []string{"v.label", "v.data.firstName", "v.data.lastName"}
  for _, s := range newFields {
    idx.AddField(s)
  }

  idx.AddDocPrefix(doc["gid"], doc, "v.")

  closeIndex()


}
