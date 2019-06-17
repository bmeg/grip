package mongo

import (
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
)

func TestQuerySizeLimit(t *testing.T) {
	Q := &gripql.Query{}
	ids := []string{}
	i := 0
	for i < 1000000 {
		ids = append(ids, util.UUID())
		i++
	}
	Q = Q.V(ids...)

	c := NewCompiler(&Graph{})
	_, err := c.Compile(Q.Statements)
	t.Log(err)
	if err == nil {
		t.Error("expected an error on compile")
	}
}
