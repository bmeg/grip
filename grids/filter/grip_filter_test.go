package filter_test

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/grids/filter"
	"github.com/bmeg/grip/gripql"
)

func TestHasExpressions(t *testing.T) {
	// mock vertex representing {"_id": "Character:1", "_label": "Character", "height": null, "eye_color": "brown", "occupation": "farmer"}
	// packed row:
	packedRow := []byte(`{"0":{"eye_color":"brown","occupation":"farmer","height":null},"1":"Character:1"}`)
	tableName := "v_Character"

	fmt.Println("=== Testing eq nil ===")
	// eq("height", None)
	exprEqNil := gripql.Eq("height", nil)
	fmt.Println("height == None (should be true for null):", filter.MatchesHasExpression(packedRow, exprEqNil, tableName))

	// not(eq("height", None))
	exprNotEqNil := gripql.Not(gripql.Eq("height", nil))
	fmt.Println("not(height == None) (should be false for null):", filter.MatchesHasExpression(packedRow, exprNotEqNil, tableName))

	fmt.Println("=== Testing neq ===")
	// not(eq("_id", "Character:1")) -> using Eq then Not because Neq is essentially Not(Eq) in gripql filters
	exprNeqId := gripql.Not(gripql.Eq("_id", "Character:1"))
	fmt.Println("_id != Character:1 (should be false):", filter.MatchesHasExpression(packedRow, exprNeqId, tableName))

	// not(eq("_label", "Character"))
	exprNeqLabel := gripql.Not(gripql.Eq("_label", "Character"))
	fmt.Println("_label != Character (should be false):", filter.MatchesHasExpression(packedRow, exprNeqLabel, tableName))

	fmt.Println("=== Testing without ===")
	// without("eye_color", ["brown"])
	exprWithout := gripql.Without("eye_color", []any{"brown"})
	fmt.Println("without eye_color=brown (should be false):", filter.MatchesHasExpression(packedRow, exprWithout, tableName))

	// without("occupation", 0) -> checking against wrong type
	exprWithoutType := gripql.Without("occupation", []any{0})
	fmt.Println("without occupation=0 (should be true):", filter.MatchesHasExpression(packedRow, exprWithoutType, tableName))

	fmt.Println("=== Testing and ===")
	// and(eq("_label", "Character"), eq("eye_color", "brown"))
	exprAnd := gripql.And(gripql.Eq("_label", "Character"), gripql.Eq("eye_color", "brown"))
	fmt.Println("label==Character AND eye_color==brown (should be true):", filter.MatchesHasExpression(packedRow, exprAnd, tableName))
}
