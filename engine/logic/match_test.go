package logic

import (
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

func TestMatchesConditionWildcardListPath(t *testing.T) {
	tr := (&gdbi.BaseTraveler{}).AddCurrent(&gdbi.DataElement{
		ID:    "v1",
		Label: "Observation",
		Data: map[string]any{
			"items": []any{
				map[string]any{"score": 1, "status": "final"},
				map[string]any{"score": 3, "status": "pending"},
			},
		},
		Loaded: true,
	})

	if !MatchesCondition(tr, gripql.Eq("items[*].score", 3).GetCondition()) {
		t.Fatalf("expected wildcard EQ to match any list element")
	}
	if MatchesCondition(tr, gripql.Eq("items[*].score", 2).GetCondition()) {
		t.Fatalf("expected wildcard EQ to fail when no list element matches")
	}
	if !MatchesCondition(tr, gripql.Gt("items[*].score", 2).GetCondition()) {
		t.Fatalf("expected wildcard GT to match any list element")
	}
	if MatchesCondition(tr, gripql.Neq("items[*].status", "pending").GetCondition()) {
		t.Fatalf("expected wildcard NEQ to require all elements to satisfy condition")
	}
}
