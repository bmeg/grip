package test

import (
	"testing"

	"github.com/bmeg/grip/example"
  "github.com/bmeg/grip/graphql"
)

func TestBuildGraphQLSchema(t *testing.T) {
	_, err := graphql.BuildGraphQLSchema(nil, "", "example-graph", example.SWSchema)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
