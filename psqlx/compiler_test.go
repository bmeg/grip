package psqlx

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type stubCompiler struct {
	called bool
}

func (s *stubCompiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	s.called = true
	return nil, nil
}

func TestValidateDelegatedSubsetSupported(t *testing.T) {
	q := gripql.V().HasLabel("Person").Out("knows").Limit(5).Count()
	if err := validateDelegatedStatementSubset(q.Statements); err != nil {
		t.Fatalf("expected supported subset to validate, got error: %v", err)
	}
}

func TestValidateDelegatedSubsetUnsupported(t *testing.T) {
	q := gripql.V().As("x").Select("x")
	err := validateDelegatedStatementSubset(q.Statements)
	if err == nil {
		t.Fatalf("expected unsupported statement error")
	}
	if !strings.Contains(err.Error(), "as") {
		t.Fatalf("expected error to mention unsupported statement, got: %v", err)
	}
}

func TestDelegatedCompilerGuardsWhenEnabled(t *testing.T) {
	base := &stubCompiler{}
	c := newDelegatedCompiler(base, Config{DelegateTraversal: true}, "test-graph", nil)
	q := gripql.V().As("x")

	_, err := c.Compile(q.Statements, nil)
	if err == nil {
		t.Fatalf("expected compile error for unsupported delegated statement")
	}
	if base.called {
		t.Fatalf("base compiler should not be called when guard fails")
	}
}

func TestDelegatedCompilerFallsThroughWhenDisabled(t *testing.T) {
	base := &stubCompiler{}
	c := newDelegatedCompiler(base, Config{DelegateTraversal: false}, "test-graph", nil)
	q := gripql.V().As("x")

	_, err := c.Compile(q.Statements, nil)
	if err != nil {
		t.Fatalf("expected fallback compile with no guard error, got: %v", err)
	}
	if !base.called {
		t.Fatalf("expected base compiler to be called")
	}
}

func TestBuildDelegatedRequestSerializesQuerySet(t *testing.T) {
	q := gripql.V("v1").HasLabel("Person").Limit(1)
	req, err := buildDelegatedRequest(
		Config{ExtensionSchema: "grip_ext", ExtensionFunction: "grip_exec"},
		q.Statements,
	)
	if err != nil {
		t.Fatalf("expected delegated request to serialize, got: %v", err)
	}
	if req.Schema != "grip_ext" || req.Function != "grip_exec" {
		t.Fatalf("unexpected request target: %#v", req)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(req.Query, &payload); err != nil {
		t.Fatalf("expected query payload to be valid json, got: %v", err)
	}
	if _, ok := payload["query"]; !ok {
		t.Fatalf("expected serialized payload to include query field")
	}
}

func TestDelegatedCompilerRequireDelegationErrors(t *testing.T) {
	base := &stubCompiler{}
	c := newDelegatedCompiler(base, Config{DelegateTraversal: true, RequireDelegation: true, ExtensionSchema: "grip_ext", ExtensionFunction: "grip_exec"}, "test-graph", nil)
	q := gripql.V().HasLabel("Person")

	_, err := c.Compile(q.Statements, nil)
	if err == nil {
		t.Fatalf("expected explicit require-delegation error")
	}
	if !strings.Contains(err.Error(), "grip_ext.grip_exec") {
		t.Fatalf("expected extension target in error, got: %v", err)
	}
	if base.called {
		t.Fatalf("base compiler should not be called when delegation is required")
	}
}

func TestDecodeDelegatedRowCount(t *testing.T) {
	row := []byte(`{"result_type":"count","count":7}`)
	res, err := decodeDelegatedRow(row)
	if err != nil {
		t.Fatalf("expected count row decode to succeed, got: %v", err)
	}
	if res.GetCount() != 7 {
		t.Fatalf("expected count 7, got %d", res.GetCount())
	}
}

func TestDecodeDelegatedRowVertex(t *testing.T) {
	row := []byte(`{"result_type":"vertex","vertex":{"id":"v1","label":"Person","data":{"name":"a"}}}`)
	res, err := decodeDelegatedRow(row)
	if err != nil {
		t.Fatalf("expected vertex row decode to succeed, got: %v", err)
	}
	v := res.GetVertex()
	if v == nil || v.Id != "v1" || v.Label != "Person" {
		t.Fatalf("unexpected vertex payload: %#v", v)
	}
}

func TestDecodeDelegatedRowError(t *testing.T) {
	row := []byte(`{"result_type":"error","error":{"code":"BAD","message":"nope"}}`)
	_, err := decodeDelegatedRow(row)
	if err == nil {
		t.Fatalf("expected extension error row to return error")
	}
	if !strings.Contains(err.Error(), "BAD") {
		t.Fatalf("expected error code in message, got: %v", err)
	}
}
