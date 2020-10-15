package grids

import (
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/setcmp"
)

// Compiler gets a compiler that will use the graph the execute the compiled query
func (ggraph *Graph) Compiler() gdbi.Compiler {
	return NewCompiler(ggraph)
}

type Compiler struct {
	graph *Graph
}

func NewCompiler(ggraph *Graph) gdbi.Compiler {
	return Compiler{graph: ggraph}
}

func (comp Compiler) Compile(stmts []*gripql.GraphStatement) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &core.DefaultPipeline{}, nil
	}

	stmts = core.Flatten(stmts)

	if err := core.Validate(stmts); err != nil {
		return &core.DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	stmts = core.IndexStartOptimize(stmts)

	ps := pipeline.NewPipelineState(stmts)

	noLoadPaths := inspect.PipelineNoLoadPath(stmts, 2)
	procs := make([]gdbi.Processor, 0, len(stmts))

	//log.Printf("Starting Grids Compiler: %s", stmts)
	for i := 0; i < len(stmts); i++ {
		foundPath := -1
		for p := range noLoadPaths {
			if setcmp.ContainsInt(noLoadPaths[p], i) {
				foundPath = p
			}
		}
		optimized := false
		if foundPath != -1 {
			//log.Printf("Compile Statements: %s", noLoadPaths[foundPath])
			curPathSteps := noLoadPaths[foundPath]
			path := SelectPath(stmts, curPathSteps)
			//log.Printf("Compile step %d: %s (%s)", i, path, curPathSteps)
			p, err := RawPathCompile(comp.graph, ps, path)
			if err == nil {
				procs = append(procs, p)
				i = curPathSteps[len(curPathSteps)-1]
				optimized = true
				//fmt.Printf("Pathway out: %s\n", ps.LastType)
			} else {
				//BUG: if there is a failure, the pipline state may contain variables from the aborted pipeline optimization
				log.Errorf("Failure optimizing pipeline")
				//something went wrong and we'll skip optimizing this path
				tmp := [][]int{}
				for j := range noLoadPaths {
					if j != foundPath {
						tmp = append(tmp, noLoadPaths[j])
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
