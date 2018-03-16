
package test

import (
  "os"
  "log"
  "testing"
  "strings"
  "github.com/bmeg/arachne/kvgraph"
  "github.com/bmeg/arachne/badgerdb"
  "github.com/bmeg/arachne/aql"
  "github.com/golang/protobuf/jsonpb"
)

var testGraph = `{
  "vertices" : [
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
    "label" : "Person",
    "data" : {
      "firstName" : "Jane",
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
  },{
  	"gid" : "vertex3",
    "label" : "Cat",
    "data" : {
      "firstName" : "Felix",
      "lastName" : "Paws",
      "age" : 2
    }
  }],
  "edges" : [

  ]
}
`


func setupGraph() kvgraph.KVGraph {
	kv, _ := badgerdb.BadgerBuilder("test.db")
	return kv
}

func closeGraph(kv kvgraph.KVGraph) {
  kv.Close()
	os.RemoveAll("test.db")
}


func TestVertexLabel(b *testing.T) {
		e := aql.Graph{}
		if err := jsonpb.Unmarshal(strings.NewReader(testGraph), &e); err != nil {
			log.Printf("Error: %s", err)
		}

    kv := setupGraph()
		kv.AddSubGraph("testGraph", e)

    closeGraph(kv)

}
