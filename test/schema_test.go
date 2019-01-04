package test

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
)

func TestSchema(t *testing.T) {
	if dbname == "psql" || dbname == "existing-sql" || dbname == "elastic" {
		t.Skip("skipping schema test")
	}

	schema, err := gdb.BuildSchema(context.Background(), "test-graph", 10, false)
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
	err = gi.AddVertex(schema.Vertices)
	if err != nil {
		t.Fatal(err)
	}
	err = gi.AddEdge(schema.Edges)
	if err != nil {
		t.Fatal(err)
	}
	Q := &gripql.Query{}
	pipeline, err := gi.Compiler().Compile(Q.V().HasLabel("users").Statements)
	if err != nil {
		t.Fatal(err)
	}
	workdir := "./test.workdir." + util.RandomString(6)
	defer os.RemoveAll(workdir)
	res := engine.Run(context.Background(), pipeline, workdir)
	expected := &gripql.QueryResult{
		Result: &gripql.QueryResult_Vertex{
			Vertex: &gripql.Vertex{
				Gid:   "users",
				Label: "users",
				Data: protoutil.AsStruct(map[string]interface{}{
					"created_at": "STRING",
					"deleted_at": "UNKNOWN",
					"details":    "UNKNOWN",
					"email":      "STRING",
					"id":         "NUMERIC",
					"password":   "STRING",
				}),
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
