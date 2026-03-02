package grids

import (
	"testing"

	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
	"google.golang.org/protobuf/types/known/structpb"
)

func testEQHasExpr(t *testing.T, field string, value any) *gripql.HasExpression {
	t.Helper()
	v, err := structpb.NewValue(value)
	if err != nil {
		t.Fatalf("structpb.NewValue(%v): %v", value, err)
	}
	return &gripql.HasExpression{
		Expression: &gripql.HasExpression_Condition{
			Condition: &gripql.HasCondition{
				Condition: gripql.Condition_EQ,
				Key:       field,
				Value:     v,
			},
		},
	}
}

func TestGridsOptimizerFusesHasLabelHasOut(t *testing.T) {
	pipe := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasLabel{HasLabel: protoutil.NewListFromStrings([]string{"Observation"})}},
		{Statement: &gripql.GraphStatement_Has{Has: testEQHasExpr(t, "status", "final")}},
		{Statement: &gripql.GraphStatement_Out{Out: protoutil.NewListFromStrings([]string{"derivedFrom"})}},
		{Statement: &gripql.GraphStatement_Limit{Limit: 10}},
	}

	optimized := GridsOptimizer(pipe)
	if len(optimized) != 2 {
		t.Fatalf("expected 2 statements after fusion, got %d", len(optimized))
	}

	customStmt, ok := optimized[0].GetStatement().(*gripql.GraphStatement_EngineCustom)
	if !ok {
		t.Fatalf("first statement was not EngineCustom: %T", optimized[0].GetStatement())
	}
	step, ok := customStmt.Custom.(lookupVertsCondIndexTraverseStep)
	if !ok {
		t.Fatalf("unexpected custom step type: %T", customStmt.Custom)
	}
	if step.inbound {
		t.Fatalf("expected outbound traversal")
	}
	if step.emitNull {
		t.Fatalf("expected non-null emitting traversal")
	}
	if len(step.labels) != 1 || step.labels[0] != key.VertexTablePrefix+"Observation" {
		t.Fatalf("unexpected labels: %#v", step.labels)
	}
	if len(step.edgeLabels) != 1 || step.edgeLabels[0] != "derivedFrom" {
		t.Fatalf("unexpected edge labels: %#v", step.edgeLabels)
	}
	if _, ok := optimized[1].GetStatement().(*gripql.GraphStatement_Limit); !ok {
		t.Fatalf("expected trailing limit to be preserved, got %T", optimized[1].GetStatement())
	}
}

func TestGridsOptimizerFusesHasInNull(t *testing.T) {
	pipe := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{Has: testEQHasExpr(t, "auth_resource_path", "/Patient/123")}},
		{Statement: &gripql.GraphStatement_InNull{InNull: protoutil.NewListFromStrings([]string{"subject"})}},
	}

	optimized := GridsOptimizer(pipe)
	if len(optimized) != 1 {
		t.Fatalf("expected 1 statement after fusion, got %d", len(optimized))
	}

	customStmt, ok := optimized[0].GetStatement().(*gripql.GraphStatement_EngineCustom)
	if !ok {
		t.Fatalf("first statement was not EngineCustom: %T", optimized[0].GetStatement())
	}
	step, ok := customStmt.Custom.(lookupVertsCondIndexTraverseStep)
	if !ok {
		t.Fatalf("unexpected custom step type: %T", customStmt.Custom)
	}
	if !step.inbound {
		t.Fatalf("expected inbound traversal")
	}
	if !step.emitNull {
		t.Fatalf("expected null-emitting traversal")
	}
	if len(step.labels) != 0 {
		t.Fatalf("expected no explicit labels, got %#v", step.labels)
	}
	if len(step.edgeLabels) != 1 || step.edgeLabels[0] != "subject" {
		t.Fatalf("unexpected edge labels: %#v", step.edgeLabels)
	}
}
