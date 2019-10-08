package test

import (
  "fmt"
  "testing"
  "github.com/bmeg/grip/grids"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/engine/inspect"

)


func SelectPath(stmts []*gripql.GraphStatement, steps []string, path []string) []*gripql.GraphStatement {
  out := []*gripql.GraphStatement{}
  for _, p := range path {
    for i := range steps {
      if steps[i] == p {
        out = append(out, stmts[i])
      }
    }
  }
  return out
}


func TestPath2Step(t *testing.T) {
  q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))


  steps := inspect.PipelineSteps(q.Statements)
  noLoadPaths := inspect.PipelineNoLoadPathSteps(q.Statements, 2)

  if len(noLoadPaths) > 0 {
  	fmt.Printf("Found Path: %s\n", noLoadPaths)
    path := SelectPath(q.Statements, steps, noLoadPaths[0])
    proc := grids.RawPathCompile( nil, path )
    fmt.Printf("Proc: %s\n", proc)
  }
}
