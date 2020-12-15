package gdbi

import (
	"context"

	"github.com/bmeg/grip/gripql"
)

type PipelineState interface {
	GetLastType() DataType
	SetLastType(DataType)
}

type CustomProcGen interface {
	GetType() DataType
	GetProcessor(db GraphInterface, ps PipelineState) (Processor, error)
}

// Compiler takes a gripql query and turns it into an executable pipeline
type Compiler interface {
	Compile(stmts []*gripql.GraphStatement) (Pipeline, error)
}

// Processor is the interface for a step in the pipe engine
type Processor interface {
	Process(ctx context.Context, man Manager, in InPipe, out OutPipe) context.Context
}

// Pipeline represents a set of processors
type Pipeline interface {
	Processors() []Processor
	DataType() DataType
	MarkTypes() map[string]DataType
}
