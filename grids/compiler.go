package grids

import (
	"fmt"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// Compiler gets a compiler that will use the graph the execute the compiled query
func (ggraph *GridsGraph) Compiler() gdbi.Compiler {
	return NewCompiler(ggraph)
}

type GridsCompiler struct {
	graph *GridsGraph
}

func NewCompiler(ggraph *GridsGraph) gdbi.Compiler {
	return GridsCompiler{graph: ggraph}
}

func (comp GridsCompiler) Compile(stmts []*gripql.GraphStatement) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &core.DefaultPipeline{}, nil
	}

	stmts = core.Flatten(stmts)

	if err := core.Validate(stmts); err != nil {
		return &core.DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	stmts = core.IndexStartOptimize(stmts)

	ps := gdbi.NewPipelineState(stmts)

	noLoadPaths := inspect.PipelineNoLoadPathSteps(stmts, 2)
	if len(noLoadPaths) > 0 {
		fmt.Printf("Found Path: %s\n", noLoadPaths)
		//stmts = append(stmts, &gripql.GraphStatement{&gripql.GraphStatement_EngineCustom{"path", PathStatement{}}})
	}

	procs := make([]gdbi.Processor, 0, len(stmts))

	for i, gs := range stmts {
		ps.SetCurStatment(i)
		p, err := core.StatementProcessor(gs, comp.graph, ps)
		if err != nil {
			return &core.DefaultPipeline{}, err
		}
		procs = append(procs, p)
	}
	return core.NewPipeline(procs, ps), nil
}
