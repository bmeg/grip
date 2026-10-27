package mongo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSameAsUsesMongoPipeline(t *testing.T) {
	query := gripql.NewQuery().
		V().
		As("start").
		Out("FRIEND").
		As("middle").
		Out("FRIEND").
		SameAs("start")
	pipe, err := NewCompiler(&Graph{}).Compile(query.Statements, nil)
	if err != nil {
		t.Fatalf("failed to compile SameAs query: %v", err)
	}
	if len(pipe.Processors()) != 1 {
		t.Fatalf("expected a single Mongo pipeline processor, got %d", len(pipe.Processors()))
	}
	processor, ok := pipe.Processors()[0].(*Processor)
	if !ok {
		t.Fatalf("expected native Mongo processor, got %T", pipe.Processors()[0])
	}
	pipelineJSON, err := bson.MarshalExtJSON(bson.M{"pipeline": processor.query}, false, false)
	if err != nil {
		t.Fatalf("failed to serialize Mongo pipeline: %v", err)
	}
	text := string(pipelineJSON)
	if !strings.Contains(text, `"$marks.start.data._id"`) || !strings.Contains(text, `"$data._id"`) {
		t.Fatalf("pipeline does not compare the current and bound element IDs: %s", text)
	}
	for _, mark := range []string{`"marks.start":"$data"`, `"marks.middle":"$data"`} {
		if !strings.Contains(text, mark) {
			t.Fatalf("pipeline does not retain/update binding %s: %s", mark, text)
		}
	}

	edgeQuery := gripql.NewQuery().V().As("node").OutE("FRIEND").As("edge").SameAs("edge")
	edgePipe, err := NewCompiler(&Graph{}).Compile(edgeQuery.Statements, nil)
	if err != nil {
		t.Fatalf("failed to compile edge SameAs query: %v", err)
	}
	edgeProcessor := edgePipe.Processors()[0].(*Processor)
	edgeJSON, err := bson.MarshalExtJSON(bson.M{"pipeline": edgeProcessor.query}, false, false)
	if err != nil {
		t.Fatalf("failed to serialize edge Mongo pipeline: %v", err)
	}
	if !strings.Contains(string(edgeJSON), `"$marks.edge.data._id"`) {
		t.Fatalf("edge SameAs pipeline does not compare the bound edge ID: %s", edgeJSON)
	}
}

func TestSameAsMongoCompilerValidation(t *testing.T) {
	tests := []struct {
		name  string
		query *gripql.Query
		want  string
	}{
		{
			name:  "unknown binding",
			query: gripql.NewQuery().V().SameAs("missing"),
			want:  `references unknown binding "missing"`,
		},
		{
			name:  "mismatched element kind",
			query: gripql.NewQuery().V().As("vertex").OutE().SameAs("vertex"),
			want:  `binding "vertex" has type VertexData, current element has type EdgeData`,
		},
		{
			name:  "requires an element traveler",
			query: gripql.NewQuery().SameAs("node"),
			want:  `"sameAs" statement is only valid for edge or vertex types`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewCompiler(&Graph{}).Compile(tc.query.Statements, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestQuerySizeLimit(t *testing.T) {
	Q := &gripql.Query{}
	ids := []string{}
	i := 0
	for i < 1000000 {
		ids = append(ids, util.UUID())
		i++
	}
	Q = Q.V(ids...)

	c := NewCompiler(&Graph{})
	_, err := c.Compile(Q.Statements, nil)
	t.Log(err)
	if err == nil {
		t.Error("expected an error on compile")
	}
}

func TestDistinctPathing(t *testing.T) {

	fields := []string{"$case._id", "$compound._id"}

	match := bson.M{}
	keys := bson.M{}

	for _, f := range fields {
		namespace := tpath.GetNamespace(f)
		fmt.Printf("Namespace: %s\n", namespace)
		f = tpath.NormalizePath(f)
		f = strings.TrimPrefix(f, "$.")
		if f == "id" {
			f = FIELD_ID
		}
		if namespace != tpath.CURRENT {
			f = fmt.Sprintf("marks.%s.%s", namespace, f)
		}
		match[f] = bson.M{"$exists": true}
		k := strings.Replace(f, ".", "_", -1)
		keys[k] = "$" + f
	}
	if m, ok := match["marks.case._id"]; ok {
		m1 := m.(bson.M)
		if e, ok := m1["$exists"]; ok {
			if b, ok := e.(bool); ok {
				if !b {
					t.Errorf("$exist value incorrect")
				}
			} else {
				t.Errorf("$exist value incorrect")
			}
		} else {
			t.Errorf("Mark key not formatted correctly")
		}
	} else {
		t.Errorf("mark key not found")
	}
}
