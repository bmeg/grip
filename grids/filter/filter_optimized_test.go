package filter

import (
	"testing"

	"github.com/bmeg/grip/gripql"
)

func TestMatchesRawPayloadNumericComparisons(t *testing.T) {
	row := `{"value":15,"text":"abc"}`

	f1 := &GripQLFilter{Expression: gripql.Gt("value", 10)}
	if !f1.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected value > 10 to match")
	}

	f2 := &GripQLFilter{Expression: gripql.Gte("value", 15)}
	if !f2.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected value >= 15 to match")
	}

	// Non-numeric lookups with numeric predicates should be false.
	f3 := &GripQLFilter{Expression: gripql.Gt("text", 10)}
	if f3.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected text > 10 to not match")
	}
}

func TestMatchesRawPayloadWithinWithoutSets(t *testing.T) {
	row := `{"status":"running","score":2}`

	f1 := &GripQLFilter{Expression: gripql.Within("status", "running", "queued")}
	if !f1.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected within(status) to match")
	}

	f2 := &GripQLFilter{Expression: gripql.Without("status", "done", "cancelled")}
	if !f2.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected without(status) to match")
	}

	f3 := &GripQLFilter{Expression: gripql.Within("score", 1, 2, 3)}
	if !f3.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected within(score) to match")
	}

	f4 := &GripQLFilter{Expression: gripql.Without("score", 1, 2, 3)}
	if f4.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected without(score) to not match")
	}
}

func TestMatchesRawPayloadContains(t *testing.T) {
	row := `{"tags":["a","b","c"],"nums":[1,2,3]}`

	f1 := &GripQLFilter{Expression: gripql.Contains("tags", "b")}
	if !f1.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected contains(tags, b) to match")
	}

	f2 := &GripQLFilter{Expression: gripql.Contains("nums", 2)}
	if !f2.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected contains(nums, 2) to match")
	}
}

func TestMatchesRawPayloadWildcardListPath(t *testing.T) {
	row := `{"items":[{"score":1},{"score":3},{"score":5}]}`

	f1 := &GripQLFilter{Expression: gripql.Eq("items[*].score", 3)}
	if !f1.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected wildcard EQ to match")
	}

	f2 := &GripQLFilter{Expression: gripql.Gt("items[*].score", 4)}
	if !f2.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected wildcard GT to match")
	}

	f3 := &GripQLFilter{Expression: gripql.Eq("items[*].score", 2)}
	if f3.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected wildcard EQ to fail when no list element matches")
	}

	f4 := &GripQLFilter{Expression: gripql.Eq("items.*.score", 1)}
	if !f4.MatchesRawPayload("v1", row, "v_Test") {
		t.Fatalf("expected dot-wildcard EQ to match")
	}
}

func TestMatchesRawPayloadNestedWildcardListPath(t *testing.T) {
	row := `{
		"component":[
			{"code":{"coding":[{"code":"File_Format"}]}},
			{"code":{"coding":[{"code":"Atlas_Name"}]}}
		]
	}`

	f1 := &GripQLFilter{Expression: gripql.Eq("component[*].code.coding[*].code", "File_Format")}
	if !f1.MatchesRawPayload("v1", row, "v_Observation") {
		t.Fatalf("expected nested wildcard EQ to match")
	}

	f2 := &GripQLFilter{Expression: gripql.Eq("component[*].code.coding[*].code", "not_present")}
	if f2.MatchesRawPayload("v1", row, "v_Observation") {
		t.Fatalf("expected nested wildcard EQ to fail when no nested element matches")
	}
}
