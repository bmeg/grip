
package gdbi

import (
  "context"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/engine/inspect"
)

type PipelineState struct {
	LastType DataType
	MarkTypes map[string]DataType
	Steps    []string
	StepOutputs map[string][]string
	CurStep   string
}

func (ps *PipelineState) SetCurStatment(a int) {
	ps.CurStep = ps.Steps[a]
}

func (ps *PipelineState) StepLoadData() bool {
	if x, ok := ps.StepOutputs[ps.CurStep]; ok {
		if len(x) == 1 && x[0] == "_label" {
			return false
		}
		return true
	}
	return false
}

func NewPipelineState(stmts []*gripql.GraphStatement) *PipelineState {
	steps := inspect.PipelineSteps(stmts)
	stepOut := inspect.PipelineStepOutputs(stmts)

	return &PipelineState{
		LastType: NoData,
		MarkTypes: map[string]DataType{},
		Steps: steps,
		StepOutputs: stepOut,
	}
}


type CustomProcGen interface {
  GetProcessor(db GraphInterface, ps *PipelineState) (Processor, error)
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
