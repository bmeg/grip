package test

import (
  "os"
  "fmt"
  "testing"
  "github.com/bmeg/grip/gdbi"
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

  ps := gdbi.NewPipelineState(q.Statements)

  steps := inspect.PipelineSteps(q.Statements)
  noLoadPaths := inspect.PipelineNoLoadPathSteps(q.Statements, 2)

  if len(noLoadPaths) > 0 {
  	fmt.Printf("Found Path: %s\n", noLoadPaths)
    path := SelectPath(q.Statements, steps, noLoadPaths[0])
    proc := grids.RawPathCompile( nil, ps, path )
    fmt.Printf("Proc: %s\n", proc)
  }
}

func TestEngineQuery(t *testing.T) {
  gdb, err := grids.NewGridsGraphDB("testing.db")
  if err != nil {
    t.Error(err)
  }

  gdb.AddGraph("test")
  graph, err := gdb.Graph("test")
  if err != nil {
    t.Error(err)
  }
  comp := graph.Compiler()

  fmt.Printf("Compiler: %s\n", comp)

  gdb.Close()
  os.RemoveAll("testing.db")
}
