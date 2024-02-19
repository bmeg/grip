package gdbi

import (
	//"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/gripql/inspect"
)

type State struct {
	LastType    DataType
	MarkTypes   map[string]DataType
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

func (ps *State) GetLastType() DataType {
	return ps.LastType
}

func (ps *State) SetLastType(a DataType) {
	ps.LastType = a
}

func NewPipelineState(stmts []*gripql.GraphStatement, storeMarks bool) *State {
	steps := inspect.PipelineSteps(stmts)
	stepOut := inspect.PipelineStepOutputs(stmts, storeMarks)

	return &State{
		LastType:    NoData,
		MarkTypes:   map[string]DataType{},
		Steps:       steps,
		StepOutputs: stepOut,
	}
}
