package arango

import (
	"strings"
	"testing"
)

func TestForLoopString(t *testing.T) {

	expected := `
FOR v0 IN [DOCUMENT("Vertices", "Character:1")]
  FOR v1, e1 IN 1..1 OUTBOUND v0 GraphName
    FILTER e1.label == "edge1"
    FILTER v1.color == "blue"
    FOR v2, e2 IN 1..1 OUTBOUND v1 GraphName
      FILTER e2.label == "edge2"
      RETURN v2`

	// Level 3: innermost for loop with return
	level3 := &ForLoop{
		Variables: []string{"v2", "e2"},
		Range:     "1..1",
		Direction: "OUTBOUND",
		Source:    "v1",
		GraphName: "GraphName",
		Body: ForStatement{
			Children: []Statement{
				&FilterStatement{Expr: "e2.label == \"edge2\""},
				&ReturnStatement{Variable: "v2"},
			},
		},
	}

	// Level 2: middle for loop with filter and level3 body
	level2 := &ForLoop{
		Variables: []string{"v1", "e1"},
		Range:     "1..1",
		Direction: "OUTBOUND",
		Source:    "v0",
		GraphName: "GraphName",
		Body: ForStatement{
			Children: []Statement{
				&FilterStatement{Expr: "e1.label == \"edge1\""},
				&FilterStatement{Expr: "v1.color == \"blue\""},
				level3,
			},
		},
	}

	// Level 1: root for loop with level2 body
	level1 := &ForLoop{
		Variables:  []string{"v0"},
		Collection: "[DOCUMENT(\"Vertices\", \"Character:1\")]",
		Body: ForStatement{
			Children: []Statement{level2},
		},
	}

	base := &Base{
		ForLoop: level1,
	}

	result := strings.TrimRight(base.String(), "\n")

	if result != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, result)
	}
}

func TestForSortLimitString(t *testing.T) {
	expected := `
FOR v IN Vertices
  FILTER v._label == "Character"
  SORT v.height DESC
  LIMIT 10
  RETURN v`

	// Create the ForLoop with Sort and Limit
	forLoop := &ForLoop{
		Variables:  []string{"v"},
		Collection: "Vertices",
		Body: ForStatement{
			Children: []Statement{
				&FilterStatement{Expr: "v._label == \"Character\""},
				&SortStatement{Expr: "v.height DESC"},
				&LimitStatement{Limit: 10},
				&ReturnStatement{Variable: "v"},
			},
		},
	}

	base := &Base{
		ForLoop: forLoop,
	}

	result := strings.TrimRight(base.String(), "\n")

	if result != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, result)
	}
}
