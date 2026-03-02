package gdbi

import (
	"strings"
	"testing"

	"github.com/bmeg/grip/gripql"
)

func TestStatementProcessorRejectsUnwind(t *testing.T) {
	ps := NewPipelineState([]*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_Unwind{Unwind: "items"}},
	}, false)
	var sc StatementCompiler
	_, err := StatementProcessor(sc, &gripql.GraphStatement{
		Statement: &gripql.GraphStatement_Unwind{Unwind: "items"},
	}, nil, ps)
	if err == nil {
		t.Fatalf("expected unwind to be rejected")
	}
	if !strings.Contains(err.Error(), "no longer supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}
