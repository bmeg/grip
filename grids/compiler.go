package grids

import (
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/engine/core"
)


// Compiler gets a compiler that will use the graph the execute the compiled query
func (ggraph *GridsGraph) Compiler() gdbi.Compiler {
	return NewCompiler(ggraph)
}

type GridsCompiler struct {
  graph *GridsGraph
}

func NewCompiler(ggraph *GridsGraph) gdbi.Compiler {
  return GridsCompiler{graph:ggraph}
}

func (comp GridsCompiler) Compile(stmts []*gripql.GraphStatement) (gdbi.Pipeline, error) {
  c := core.NewCompiler(comp.graph)
  return c.Compile(stmts)
}
