package test

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSchema(t *testing.T) {
	if dbname == "psql" || dbname == "existing-sql" || dbname == "elastic" {
		t.Skip("skipping schema test")
	}

	schema, err := gdb.BuildSchema(context.Background(), "test-graph", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	err = gdb.AddGraph("test-graph__schema__")
	if err != nil {
		t.Fatal(err)
	}
	gi, err := gdb.Graph("test-graph__schema__")
	if err != nil {
		t.Fatal(err)
	}
	ve := []*gdbi.Vertex{}
	for i := range schema.Vertices {
		ve = append(ve, gdbi.NewElementFromVertex(schema.Vertices[i]))
	}
	err = gi.AddVertex(ve)
	if err != nil {
		t.Fatal(err)
	}
	ee := []*gdbi.Edge{}
	for i := range schema.Edges {
		ee = append(ee, gdbi.NewElementFromEdge(schema.Edges[i]))
	}
	err = gi.AddEdge(ee)
	if err != nil {
		t.Fatal(err)
	}
	Q := &gripql.Query{}
	compiledPipeline, err := gi.Compiler().Compile(Q.V().HasLabel("users").Statements, nil)
	if err != nil {
		t.Fatal(err)
	}
	workdir := "./test.workdir." + util.RandomString(6)
	defer os.RemoveAll(workdir)
	res := pipeline.Run(context.Background(), compiledPipeline, workdir)
	ds, _ := structpb.NewStruct(map[string]interface{}{
		"created_at": "STRING",
		"deleted_at": "UNKNOWN",
		"details":    "UNKNOWN",
		"email":      "STRING",
		"id":         "NUMERIC",
		"password":   "STRING",
	})
	expected := &gripql.QueryResult{
		Result: &gripql.QueryResult_Vertex{
			Vertex: &gripql.Vertex{
				Gid:   "users",
				Label: "users",
				Data:  ds,
			},
		},
	}
	for r := range res {
		if !reflect.DeepEqual(r, expected) {
			t.Logf("actual: %+v", r)
			t.Logf("expected: %+v", expected)
			t.Error("unexpected traversal result")
		}
	}
}
