package core

import (
	"context"
	"strings"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

func TestSameAsFilter(t *testing.T) {
	matching := &gdbi.BaseTraveler{
		Current: &gdbi.DataElement{ID: "same"},
		Marks:   map[string]*gdbi.DataElement{"bound": {ID: "same"}},
	}
	different := &gdbi.BaseTraveler{
		Current: &gdbi.DataElement{ID: "different"},
		Marks:   map[string]*gdbi.DataElement{"bound": {ID: "same"}},
	}
	missing := &gdbi.BaseTraveler{Current: &gdbi.DataElement{ID: "same"}}

	in := make(chan gdbi.Traveler, 3)
	in <- matching
	in <- different
	in <- missing
	close(in)

	out := make(chan gdbi.Traveler)
	(&SameAsFilter{mark: "bound"}).Process(context.Background(), nil, in, out)

	var results []gdbi.Traveler
	for traveler := range out {
		results = append(results, traveler)
	}
	if len(results) != 1 || results[0] != matching {
		t.Fatalf("expected only the traveler matching its binding, got %#v", results)
	}
}

func TestSameAsCompileErrors(t *testing.T) {
	tests := []struct {
		name  string
		query *gripql.Query
		want  string
	}{
		{
			name:  "unknown binding",
			query: gripql.NewQuery().V().SameAs("missing"),
			want:  `unknown binding "missing"`,
		},
		{
			name:  "mismatched element kind",
			query: gripql.NewQuery().V().As("vertex").OutE().SameAs("vertex"),
			want:  `binding "vertex" has type VertexData, current element has type EdgeData`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewCompiler(nil).Compile(tc.query.Statements, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}
