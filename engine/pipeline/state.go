package pipeline

import (
	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type State struct {
	LastType    gdbi.DataType
	MarkTypes   map[string]gdbi.DataType
	Steps       []string
	StepOutputs map[string][]string
	CurStep     string
}

func (ps *State) SetCurStatment(a int) {
	ps.CurStep = ps.Steps[a]
}

func (ps *State) StepLoadData() bool {
	if x, ok := ps.StepOutputs[ps.CurStep]; ok {
		if len(x) == 1 && x[0] == "_label" {
			return false
		}
		return true
	}
	return false
}

func (ps *State) GetLastType() gdbi.DataType {
	return ps.LastType
}

func (ps *State) SetLastType(a gdbi.DataType) {
	ps.LastType = a
}

func NewPipelineState(stmts []*gripql.GraphStatement) *State {
	steps := inspect.PipelineSteps(stmts)
	stepOut := inspect.PipelineStepOutputs(stmts)

	return &State{
		LastType:    gdbi.NoData,
		MarkTypes:   map[string]gdbi.DataType{},
		Steps:       steps,
		StepOutputs: stepOut,
	}
}
