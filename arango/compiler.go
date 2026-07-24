package arango

import (
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

func (g *Graph) NewTranspiler() (gdbi.Compiler, error) {
	return &Transpiler{graph: g, graphName: g.graphName}, nil
}

type Transpiler struct {
	graph     *Graph
	graphName string
}

// Compile implements [gdbi.Compiler].
func (t *Transpiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &Pipeline{graph: t.graph, procs: []gdbi.Processor{}, dataType: gdbi.NoData, markTypes: map[string]gdbi.DataType{}}, nil
	}

	// The transpiler does not support extending an existing traveler stream yet.
	if opts != nil && opts.Extends != nil {
		log.Info("Skipping arango transpiler")
		cmpl := core.NewCompiler(t.graph)
		return cmpl.Compile(stmts, opts)
	}

	prefixLen := 0
	for ; prefixLen < len(stmts); prefixLen++ {
		if !isTranspilableStatement(stmts[prefixLen]) {
			break
		}
	}

	// If the first statement is not supported by the transpiler, run everything in core.
	if prefixLen == 0 {
		cmpl := core.NewCompiler(t.graph)
		return cmpl.Compile(stmts, opts)
	}

	includePathPayload := hasPathStatement(stmts[prefixLen:])

	ast, err := TranslatePipeline(stmts[:prefixLen], t.graphName, includePathPayload)
	if err != nil {
		return nil, err
	}

	procs := []gdbi.Processor{&Processor{db: t.graph, ast: ast}}
	markTypes := map[string]gdbi.DataType{}
	lastType := gdbi.VertexData

	if prefixLen < len(stmts) {
		cmpl := core.NewCompiler(t.graph)
		extOpts := &gdbi.CompileOptions{
			Extends: &gdbi.PipelineExtension{
				StartType:  lastType,
				MarksTypes: markTypes,
			},
		}
		if opts != nil {
			extOpts.StoreMarks = opts.StoreMarks
		}

		extPipe, err := cmpl.Compile(stmts[prefixLen:], extOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to compile statements after transpiled prefix: %w", err)
		}

		procs = append(procs, extPipe.Processors()...)
		lastType = extPipe.DataType()
		markTypes = extPipe.MarkTypes()
	}

	return &Pipeline{graph: t.graph, procs: procs, dataType: lastType, markTypes: markTypes}, nil
}

func isTranspilableStatement(gs *gripql.GraphStatement) bool {
	switch gs.GetStatement().(type) {
	case *gripql.GraphStatement_V,
		*gripql.GraphStatement_HasLabel,
		*gripql.GraphStatement_Out,
		*gripql.GraphStatement_In,
		*gripql.GraphStatement_Both,
		*gripql.GraphStatement_Limit,
		*gripql.GraphStatement_Skip,
		*gripql.GraphStatement_Range,
		*gripql.GraphStatement_Sort:
		return true
	default:
		return false
	}
}

func hasPathStatement(stmts []*gripql.GraphStatement) bool {
	for _, gs := range stmts {
		if _, ok := gs.GetStatement().(*gripql.GraphStatement_Path); ok {
			return true
		}
	}
	return false
}

// Pipeline a set of runnable query operations
type Pipeline struct {
	graph     gdbi.GraphInterface
	procs     []gdbi.Processor
	dataType  gdbi.DataType
	markTypes map[string]gdbi.DataType
}

// DataType return the datatype
func (pipe *Pipeline) DataType() gdbi.DataType {
	return pipe.dataType
}

// MarkTypes get the mark types
func (pipe *Pipeline) MarkTypes() map[string]gdbi.DataType {
	return pipe.markTypes
}

// Processors gets the list of processors
func (pipe *Pipeline) Processors() []gdbi.Processor {
	return pipe.procs
}

// Graph gets the graph interface
func (pipe *Pipeline) Graph() gdbi.GraphInterface {
	return pipe.graph
}
