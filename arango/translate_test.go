package arango

import (
	"strings"
	"testing"

	"github.com/bmeg/grip/gripql"
)

func assertTranslatedAQL(t *testing.T, query *gripql.Query, expected string) {
	t.Helper()

	ast, err := TranslatePipeline(query.Statements, "test_graph", false)
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

func TestTranslateSimpleVID(t *testing.T) {

	query := gripql.NewQuery()
	query = query.V("Node1")

	expected := `
FOR v0 IN Vertices
  FILTER v0._key == "Tm9kZTE"
  RETURN v0`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateInTraversal(t *testing.T) {
	query := gripql.NewQuery().V().HasLabel("Character").In("friends")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 INBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friends"
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateBothTraversal(t *testing.T) {
	query := gripql.NewQuery().V().HasLabel("Character").Both("friends")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 ANY v0 GRAPH 'test_graph'
    FILTER e1._label == "friends"
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSkipAndRange(t *testing.T) {
	t.Run("skip", func(t *testing.T) {
		query := gripql.NewQuery().V().Skip(3).Limit(2)

		expected := `
FOR v0 IN Vertices
  LIMIT 3, 2147483647
  LIMIT 2
  RETURN v0`

		assertTranslatedAQL(t, query, expected)
	})

	t.Run("range", func(t *testing.T) {
		query := gripql.NewQuery().V().Range(3, 5)

		expected := `
FOR v0 IN Vertices
  LIMIT 3, 2
  RETURN v0`

		assertTranslatedAQL(t, query, expected)
	})
}

func TestTranslateSimpleStep(t *testing.T) {

	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend").Limit(10)

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friend"
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
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friend"
    FOR v2, e2 IN 1..1 OUTBOUND v1 GRAPH 'test_graph'
      FILTER e2._label == "homeworld"
      RETURN v2`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateOutTraversalWithPathPayload(t *testing.T) {
	query := gripql.NewQuery().V("Film:1").Out("characters").Out("homeworld")

	expected := "\nFOR v0 IN Vertices\n" +
		"  LET p0 = [{vertex: v0._key}]\n" +
		"  FILTER v0._key == \"RmlsbTox\"\n" +
		"  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'\n" +
		"    FILTER e1._label == \"characters\"\n" +
		"    LET p1 = APPEND(p0, [{vertex: v1._key}])\n" +
		"    FOR v2, e2 IN 1..1 OUTBOUND v1 GRAPH 'test_graph'\n" +
		"      FILTER e2._label == \"homeworld\"\n" +
		"      LET p2 = APPEND(p1, [{vertex: v2._key}])\n" +
		"      RETURN {__current: v2, __path: p2}"

	ast, err := TranslatePipeline(query.Statements, "test_graph", true)
	if err != nil {
		t.Fatalf("TranslatePipeline returned error: %v", err)
	}

	result := strings.TrimRight(ast.String(), "\n")
	if result != expected {
		t.Fatalf("Expected:\n%s\nGot:\n%s", expected, result)
	}
}

func TestTranslateOutMultiLabelFilter(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out("friend", "coworker")

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friend" || e1._label == "coworker"
    RETURN v1`

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateOutWithoutLabelFilter(t *testing.T) {
	query := gripql.NewQuery()
	query = query.V().HasLabel("Character").Out()

	expected := `
FOR v0 IN Vertices
  FILTER v0._label == "Character"
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
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
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friend"
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
  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'
    FILTER e1._label == "friend"
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

func TestTranslateAsAddsMarkPayload(t *testing.T) {
	query := gripql.NewQuery().V("Character:1").As("a").Out("friend")

	expected := "\nFOR v0 IN Vertices\n" +
		"  LET m0 = {}\n" +
		"  FILTER v0._key == \"Q2hhcmFjdGVyOjE\"\n" +
		"  LET m1 = MERGE(m0, {\"a\": v0})\n" +
		"  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'\n" +
		"    FILTER e1._label == \"friend\"\n" +
		"    RETURN {__current: v1, __marks: m1}"

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSelectUsesMarkedVertex(t *testing.T) {
	query := gripql.NewQuery().V("Character:1").As("a").Out("friend").Select("a")

	expected := "\nFOR v0 IN Vertices\n" +
		"  LET m0 = {}\n" +
		"  FILTER v0._key == \"Q2hhcmFjdGVyOjE\"\n" +
		"  LET m1 = MERGE(m0, {\"a\": v0})\n" +
		"  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'\n" +
		"    FILTER e1._label == \"friend\"\n" +
		"    LET v2 = m1[\"a\"]\n" +
		"    RETURN {__current: v2, __marks: m1}"

	assertTranslatedAQL(t, query, expected)
}

func TestTranslateSelectAppendsPathStep(t *testing.T) {
	query := gripql.NewQuery().V("Film:1").As("a").Out("characters").Select("a")

	expected := "\nFOR v0 IN Vertices\n" +
		"  LET m0 = {}\n" +
		"  LET p0 = [{vertex: v0._key}]\n" +
		"  FILTER v0._key == \"RmlsbTox\"\n" +
		"  LET m1 = MERGE(m0, {\"a\": v0})\n" +
		"  FOR v1, e1 IN 1..1 OUTBOUND v0 GRAPH 'test_graph'\n" +
		"    FILTER e1._label == \"characters\"\n" +
		"    LET p1 = APPEND(p0, [{vertex: v1._key}])\n" +
		"    LET v2 = m1[\"a\"]\n" +
		"    LET p2 = APPEND(p1, [{vertex: v2._key}])\n" +
		"    RETURN {__current: v2, __path: p2, __marks: m1}"

	ast, err := TranslatePipeline(query.Statements, "test_graph", true)
	if err != nil {
		t.Fatalf("TranslatePipeline returned error: %v", err)
	}

	result := strings.TrimRight(ast.String(), "\n")
	if result != expected {
		t.Fatalf("Expected:\n%s\nGot:\n%s", expected, result)
	}
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
		{name: "out edge", query: gripql.NewQuery().V().OutE("friend")},
		{name: "out null", query: gripql.NewQuery().V().OutNull("friend")},
		{name: "distinct", query: gripql.NewQuery().V().Distinct("$.name")},
		{name: "unwind", query: gripql.NewQuery().V().Unwind("component")},
		{name: "group", query: gripql.NewQuery().V().Group(map[string]string{"people": "$person.name"})},
		{name: "totype", query: gripql.NewQuery().V().ToType("mass", "float")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := TranslatePipeline(tc.query.Statements, "test_graph", false)
			if err == nil {
				t.Fatalf("expected error for unsupported statement")
			}
			if !strings.Contains(err.Error(), "unsupported statement type") {
				t.Fatalf("expected unsupported statement type error, got: %v", err)
			}
		})
	}
}

func TestTranspilerCompileRejectsTraversalWithoutVStart(t *testing.T) {
	transpiler := &Transpiler{}
	query := gripql.NewQuery().Out()

	_, err := transpiler.Compile(query.Statements, nil)
	if err == nil {
		t.Fatalf("expected compile error for traversal without V() start")
	}
	if !strings.Contains(err.Error(), "first statement is not V()") {
		t.Fatalf("expected first statement validation error, got: %v", err)
	}
}
