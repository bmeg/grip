package arango

import (
	"strings"
	"testing"

	"github.com/bmeg/grip/gripql"
)

func assertTranslatedAQL(t *testing.T, query *gripql.Query, expected string) {
	t.Helper()

	ast, err := TranslatePipeline(query.Statements, "test_graph")
	if err != nil {
		t.Fatalf("TranslatePipeline returned error: %v", err)
	}

	result := strings.TrimRight(ast.String(), "\n")
	if result != expected {
		t.Fatalf("Expected:\n%s\nGot:\n%s", expected, result)
	}
}

func TestTranslateSimpleV(t *testing.T) {

	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Limit(10)

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  LIMIT 10
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSimpleStep(t *testing.T) {

	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend").Limit(10)

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    FILTER e1.label == "friend"
    LIMIT 10
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateHasLabelMultiValue(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Vehicle", "Starship")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Vehicle" || v0._label == "Starship"
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateTwoHopOutTraversal(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend").Out("homeworld")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    FILTER e1.label == "friend"
    FOR v2, e2 IN 1..1 OUTBOUND v1 test_graph
      FILTER e2.label == "homeworld"
      RETURN v2`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateOutMultiLabelFilter(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend", "coworker")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    FILTER e1.label == "friend" || e1.label == "coworker"
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateOutWithoutLabelFilter(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out()

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateBareV(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V()

	expected := `
FOR v0 IN Vertices
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSortAscendingAndDescending(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Sort([]*gripql.SortField{
		{Field: "name"},
		{Field: "height", Descending: true},
	})

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  SORT name ASC, height DESC
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSortAfterTraversal(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend").Sort([]*gripql.SortField{
		{Field: "born", Descending: true},
	})

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    FILTER e1.label == "friend"
    SORT born DESC
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSortSkipsNilFields(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Sort([]*gripql.SortField{
		nil,
		{Field: "name", Descending: true},
	})

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  SORT name DESC
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateLimitBeforeTraversal(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Limit(5).Out("friend")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  LIMIT 5
  FOR v1, e1 IN 1..1 OUTBOUND v0 test_graph
    FILTER e1.label == "friend"
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSortThenLimitOrder(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Sort([]*gripql.SortField{{Field: "name"}}).Limit(2)

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  SORT name ASC
  LIMIT 2
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateLimitThenSortOrder(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Limit(2).Sort([]*gripql.SortField{{Field: "name"}})

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  LIMIT 2
  SORT name ASC
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateUnsupportedSpecStepsReturnError(t *testing.T) {
	tests := []struct {
		name  string
		query *gripql.Query
	}{
		{name: "has id", query: gripql.NewQuery().V().HasID("Character:1")},
		{name: "has expression", query: gripql.NewQuery().V().Has(gripql.Eq("name", "Leia"))},
		{name: "render", query: gripql.NewQuery().V().Render([]string{"name"})},
		{name: "count", query: gripql.NewQuery().V().Count()},
		{name: "as", query: gripql.NewQuery().V().As("a")},
		{name: "select", query: gripql.NewQuery().V().As("a").Select("a")},
		{name: "out edge", query: gripql.NewQuery().V().OutE("friend")},
		{name: "out null", query: gripql.NewQuery().V().OutNull("friend")},
		{name: "in", query: gripql.NewQuery().V().In("friend")},
		{name: "both", query: gripql.NewQuery().V().Both("friend")},
		{name: "distinct", query: gripql.NewQuery().V().Distinct("$.name")},
		{name: "unwind", query: gripql.NewQuery().V().Unwind("component")},
		{name: "group", query: gripql.NewQuery().V().Group(map[string]string{"people": "$person.name"})},
		{name: "totype", query: gripql.NewQuery().V().ToType("mass", "float")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := TranslatePipeline(tc.query.Statements, "test_graph")
			if err == nil {
				t.Fatalf("expected error for unsupported statement")
			}
			if !strings.Contains(err.Error(), "unsupported statement type") {
				t.Fatalf("expected unsupported statement type error, got: %v", err)
			}
		})
	}
}
