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

type CompileOptions struct {
	//Compile pipeline extension
	PipelineExtension  DataType
	ExtensionMarkTypes map[string]DataType
}

// Compiler takes a gripql query and turns it into an executable pipeline
type Compiler interface {
	Compile(stmts []*gripql.GraphStatement, opts *CompileOptions) (Pipeline, error)
}

// Processor is the interface for a step in the pipe engine
type Processor interface {
	Process(ctx context.Context, man Manager, in InPipe, out OutPipe) context.Context
}

// Pipeline represents a set of processors
type Pipeline interface {
	Graph() GraphInterface
	Processors() []Processor
	DataType() DataType
	MarkTypes() map[string]DataType
}

type StatementCompiler interface {
	V(gs *gripql.GraphStatement_V, ps *State) (Processor, error)
	E(gs *gripql.GraphStatement_E, ps *State) (Processor, error)
	In(gs *gripql.GraphStatement_In, ps *State) (Processor, error)
	Out(gs *gripql.GraphStatement_Out, ps *State) (Processor, error)
	InNull(gs *gripql.GraphStatement_InNull, ps *State) (Processor, error)
	OutNull(gs *gripql.GraphStatement_OutNull, ps *State) (Processor, error)
	Both(gs *gripql.GraphStatement_Both, ps *State) (Processor, error)
	InE(gs *gripql.GraphStatement_InE, ps *State) (Processor, error)
	InENull(gs *gripql.GraphStatement_InENull, ps *State) (Processor, error)
	OutE(gs *gripql.GraphStatement_OutE, ps *State) (Processor, error)
	OutENull(gs *gripql.GraphStatement_OutENull, ps *State) (Processor, error)
	BothE(gs *gripql.GraphStatement_BothE, ps *State) (Processor, error)
	Has(gs *gripql.GraphStatement_Has, ps *State) (Processor, error)
	HasLabel(gs *gripql.GraphStatement_HasLabel, ps *State) (Processor, error)
	HasKey(gs *gripql.GraphStatement_HasKey, ps *State) (Processor, error)
	HasID(gs *gripql.GraphStatement_HasId, ps *State) (Processor, error)

	Limit(gs *gripql.GraphStatement_Limit, ps *State) (Processor, error)
	Skip(gs *gripql.GraphStatement_Skip, ps *State) (Processor, error)
	Range(gs *gripql.GraphStatement_Range, ps *State) (Processor, error)
	Count(gs *gripql.GraphStatement_Count, ps *State) (Processor, error)
	Distinct(gs *gripql.GraphStatement_Distinct, ps *State) (Processor, error)
	As(gs *gripql.GraphStatement_As, ps *State) (Processor, error)
	Set(gs *gripql.GraphStatement_Set, ps *State) (Processor, error)
	Increment(gs *gripql.GraphStatement_Increment, ps *State) (Processor, error)
	Mark(gs *gripql.GraphStatement_Mark, ps *State) (Processor, error)
	Jump(gs *gripql.GraphStatement_Jump, ps *State) (Processor, error)
	Select(gs *gripql.GraphStatement_Select, ps *State) (Processor, error)

	Render(gs *gripql.GraphStatement_Render, ps *State) (Processor, error)
	Path(gs *gripql.GraphStatement_Path, ps *State) (Processor, error)
	Unwind(gs *gripql.GraphStatement_Unwind, ps *State) (Processor, error)
	Fields(gs *gripql.GraphStatement_Fields, ps *State) (Processor, error)
	Aggregate(gs *gripql.GraphStatement_Aggregate, ps *State) (Processor, error)

	Custom(gs *gripql.GraphStatement, ps *State) (Processor, error)
}
