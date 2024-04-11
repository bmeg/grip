package core

import (
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// DefaultPipeline a set of runnable query operations
type DefaultPipeline struct {
	graph     gdbi.GraphInterface
	procs     []gdbi.Processor
	dataType  gdbi.DataType
	markTypes map[string]gdbi.DataType
}

func NewPipeline(graph gdbi.GraphInterface, procs []gdbi.Processor, ps *gdbi.State) *DefaultPipeline {
	return &DefaultPipeline{graph, procs, ps.LastType, ps.MarkTypes}
}

// DataType return the datatype
func (pipe *DefaultPipeline) DataType() gdbi.DataType {
	return pipe.dataType
}

// MarkTypes get the mark types
func (pipe *DefaultPipeline) MarkTypes() map[string]gdbi.DataType {
	return pipe.markTypes
}

// Processors gets the list of processors
func (pipe *DefaultPipeline) Processors() []gdbi.Processor {
	return pipe.procs
}

// Graph gets the processor graph interface
func (pipe *DefaultPipeline) Graph() gdbi.GraphInterface {
	return pipe.graph
}

// DefaultCompiler is the core compiler that works with default graph interface
type DefaultCompiler struct {
	db         gdbi.GraphInterface
	optimizers []QueryOptimizer
}

// NewCompiler creates a new compiler that runs using the provided GraphInterface
func NewCompiler(db gdbi.GraphInterface, optimizers ...QueryOptimizer) gdbi.Compiler {
	return DefaultCompiler{db: db, optimizers: optimizers}
}

type QueryOptimizer func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement

// Compile take set of statments and turns them into a runnable pipeline
func (comp DefaultCompiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &DefaultPipeline{}, nil
	}

	if err := Validate(stmts, opts); err != nil {
		return &DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	for _, o := range comp.optimizers {
		stmts = o(stmts)
	}

	storeMarks := false
	if opts != nil {
		storeMarks = opts.StoreMarks
	}

	ps := gdbi.NewPipelineState(stmts, storeMarks)
	if opts != nil {
		if opts.Extends != nil {
			ps.LastType = opts.Extends.StartType
			ps.MarkTypes = opts.Extends.MarksTypes
		}
	}

	procs := make([]gdbi.Processor, 0, len(stmts))

	sproc := &DefaultStmtCompiler{comp.db}

	for i, gs := range stmts {
		ps.SetCurStatment(i)
		p, err := gdbi.StatementProcessor(sproc, gs, comp.db, ps)
		if err != nil {
			return &DefaultPipeline{}, err
		}
		procs = append(procs, p)
	}

	return &DefaultPipeline{comp.db, procs, ps.LastType, ps.MarkTypes}, nil
}

// Validate checks pipeline for chains of statements that won't work
func Validate(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) error {
	for i, gs := range stmts {
		// Validate that the first statement is V() or E()
		if i == 0 {
			switch gs.GetStatement().(type) {
			case *gripql.GraphStatement_V, *gripql.GraphStatement_E:
			default:
				if opts == nil || opts.Extends.StartType == gdbi.NoData {
					return fmt.Errorf("first statement is not V() or E(): %s", gs)
				}
			}
		}
	}
	return nil
}
