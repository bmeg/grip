package test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/util"
	"github.com/golang/protobuf/jsonpb"
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

func TestVertexLabel(t *testing.T) {
	var gdb gdbi.GraphDB
	gdb = kvgraph.NewKVGraph(kvdriver)

	e := aql.Graph{}
	if err := jsonpb.Unmarshal(strings.NewReader(testGraph), &e); err != nil {
		t.Fatal("Failed to unmarshal test graph", err)
	}

	err := gdb.AddGraph("test")
	if err != nil {
		t.Fatal("Failed to add graph", err)
	}
	graph := gdb.Graph("test")
	graph.AddVertex(e.Vertices)
	graph.AddEdge(e.Edges)

	Q := aql.Query{}
	query := Q.V().HasLabel("Cat")

	compiler := graph.Compiler()
	pipeline, err := compiler.Compile(query.Statements)
	if err != nil {
		t.Fatal(err)
	}

	workdir := "./test.workdir." + util.RandomString(6)
	defer os.RemoveAll(workdir)
	res := engine.Run(context.Background(), pipeline, workdir)

	count := 0
	for range res {
		count++
	}
	if count != 1 {
		t.Errorf("Incorrect return count %d != %d", count, 1)
	}
	gdb.Close()
}
