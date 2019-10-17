package grids

import (
	"fmt"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	log "github.com/sirupsen/logrus"
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

	noLoadPaths := inspect.PipelineNoLoadPath(stmts, 2)
	procs := make([]gdbi.Processor, 0, len(stmts))

	for i := 0; i < len(stmts); i++ {
		foundPath := -1
		for p := range noLoadPaths {
			if containsInt(noLoadPaths[p], i) {
				foundPath = p
			}
		}
		optimized := false
		if (foundPath != -1) {
			log.Printf("Compile Statements: %s", noLoadPaths[foundPath])
			path := SelectPath(stmts, noLoadPaths[foundPath])
			log.Printf("Compile: %s", path)
    	p, err := RawPathCompile( comp.graph, ps, path )
			if err == nil {
				procs = append(procs, p)
				i += len(noLoadPaths[foundPath])-1
				optimized = true
			} else {
				//something went wrong and we'll skip optimizing this path
				tmp := [][]int{}
				for i := range noLoadPaths {
					if i != foundPath {
						tmp = append(tmp, noLoadPaths[i])
					}
				}
				noLoadPaths = tmp
			}
		}
		if !optimized {
			gs := stmts[i]
			ps.SetCurStatment(i)
			p, err := core.StatementProcessor(gs, comp.graph, ps)
			if err != nil {
				return &core.DefaultPipeline{}, err
			}
			procs = append(procs, p)
		}
	}
	return core.NewPipeline(procs, ps), nil
}
