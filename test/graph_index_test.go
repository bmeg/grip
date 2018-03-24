package test

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/golang/protobuf/jsonpb"
	"log"
	"os"
	"strings"
	"testing"
)

var testGraph = `{
  "vertices" : [{
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

func setupGraphDB() gdbi.GraphDB {
	kv, _ := badgerdb.BadgerBuilder("test.db")
	return kvgraph.NewKVGraph(kv)
}

func closeGraph(gd gdbi.GraphDB) {
	gd.Close()
	os.RemoveAll("test.db")
}

func TestVertexLabel(t *testing.T) {
	e := aql.Graph{}
	if err := jsonpb.Unmarshal(strings.NewReader(testGraph), &e); err != nil {
		log.Printf("Error: %s", err)
	}

	kv := setupGraphDB()
	kv.AddGraph("test")
	graph := kv.Graph("test")
	graph.AddVertex(e.Vertices)
	graph.AddEdge(e.Edges)

	var Q = aql.Query{}

	query := Q.V().HasLabel("Cat")

	p, err := engine.Compile(query.Statements, graph, "./workdir")
	if err != nil {
		t.Fatal(err)
	}
	res := p.Run(context.Background())
	count := 0
	for range res {
		count++
	}
	if count != 1 {
		t.Errorf("Incorrect return count %d != %d", count, 1)
	}
	closeGraph(kv)
}
