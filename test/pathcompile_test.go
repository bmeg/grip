package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"
)

var pathVertices = []string{
	`{"gid" : "1", "label" : "Person", "data" : { "name" : "bob" }}`,
	`{"gid" : "2", "label" : "Person", "data" : { "name" : "alice" }}`,
	`{"gid" : "3", "label" : "Person", "data" : { "name" : "jane" }}`,
	`{"gid" : "4", "label" : "Person", "data" : { "name" : "janet" }}`,
}

var pathEdges = []string{
	`{"gid" : "e1", "label" : "knows", "from" : "1", "to" : "2", "data" : {}}`,
	`{"gid" : "e3", "label" : "knows", "from" : "2", "to" : "3", "data" : {}}`,
	`{"gid" : "e4", "label" : "knows", "from" : "3", "to" : "4", "data" : {}}`,
}

func TestEngineQuery(t *testing.T) {
	gdb, err := grids.NewGraphDB("testing.db")
	if err != nil {
		t.Error(err)
	}

	gdb.AddGraph("test")
	graph, err := gdb.Graph("test")
	if err != nil {
		t.Error(err)
	}

	vset := []*gdbi.Vertex{}
	for _, r := range pathVertices {
		v := &gripql.Vertex{}
		err := protojson.Unmarshal([]byte(r), v)
		if err != nil {
			t.Error(err)
		}
		vset = append(vset, gdbi.NewElementFromVertex(v))
	}
	graph.AddVertex(vset)

	eset := []*gdbi.Edge{}
	for _, r := range pathEdges {
		e := &gripql.Edge{}
		err := protojson.Unmarshal([]byte(r), e)
		if err != nil {
			t.Error(err)
		}
		eset = append(eset, gdbi.NewElementFromEdge(e))
	}
	graph.AddEdge(eset)

	q := gripql.NewQuery()
	q = q.V().Out().Out().Count()
	comp := graph.Compiler()

	compiledPipeline, err := comp.Compile(q.Statements, nil)
	if err != nil {
		t.Error(err)
	}

	out := pipeline.Run(context.Background(), compiledPipeline, "./work.dir")
	for r := range out {
		fmt.Printf("result: %s\n", r)
	}

	q = gripql.NewQuery()
	q = q.V().Out().Out().OutE().Out().Count()

	compiledPipeline, err = comp.Compile(q.Statements, nil)
	if err != nil {
		t.Error(err)
	}

	out = pipeline.Run(context.Background(), compiledPipeline, "./work.dir")
	for r := range out {
		fmt.Printf("result: %s\n", r)
	}

	gdb.Close()
	os.RemoveAll("testing.db")
}
