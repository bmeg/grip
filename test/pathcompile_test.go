package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"github.com/golang/protobuf/jsonpb"
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

func TestPath2Step(t *testing.T) {
	q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))

	ps := pipeline.NewPipelineState(q.Statements)

	noLoadPaths := inspect.PipelineNoLoadPath(q.Statements, 2)

	if len(noLoadPaths) > 0 {
		fmt.Printf("Found Path: %#v\n", noLoadPaths)
		path := grids.SelectPath(q.Statements, noLoadPaths[0])
		proc, err := grids.RawPathCompile(nil, ps, path)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("Proc: %s\n", proc)
	}
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

	m := jsonpb.Unmarshaler{}

	vset := []*gripql.Vertex{}
	for _, r := range pathVertices {
		v := &gripql.Vertex{}
		err := m.Unmarshal(strings.NewReader(r), v)
		if err != nil {
			t.Error(err)
		}
		vset = append(vset, v)
	}
	graph.AddVertex(vset)

	eset := []*gripql.Edge{}
	for _, r := range pathEdges {
		e := &gripql.Edge{}
		err := m.Unmarshal(strings.NewReader(r), e)
		if err != nil {
			t.Error(err)
		}
		eset = append(eset, e)
	}
	graph.AddEdge(eset)

	q := gripql.NewQuery()
	q = q.V().Out().Out().Count()
	comp := graph.Compiler()

	compiledPipeline, err := comp.Compile(q.Statements)
	if err != nil {
		t.Error(err)
	}

	out := pipeline.Run(context.Background(), compiledPipeline, "./work.dir")
	for r := range out {
		fmt.Printf("result: %s\n", r)
	}

	q = gripql.NewQuery()
	q = q.V().Out().Out().OutE().Out().Count()

	compiledPipeline, err = comp.Compile(q.Statements)
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
