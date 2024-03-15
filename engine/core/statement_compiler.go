package core

import (
	"fmt"

	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

type DefaultStmtCompiler struct {
	db gdbi.GraphInterface
}

func (sc *DefaultStmtCompiler) V(stmt *gripql.GraphStatement_V, ps *gdbi.State) (gdbi.Processor, error) {
	ids := protoutil.AsStringList(stmt.V)
	return &LookupVerts{db: sc.db, ids: ids, loadData: ps.StepLoadData()}, nil
}

func (sc *DefaultStmtCompiler) E(stmt *gripql.GraphStatement_E, ps *gdbi.State) (gdbi.Processor, error) {
	ids := protoutil.AsStringList(stmt.E)
	return &LookupEdges{db: sc.db, ids: ids, loadData: ps.StepLoadData()}, nil
}

func (sc *DefaultStmtCompiler) In(stmt *gripql.GraphStatement_In, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.In)
	if ps.LastType == gdbi.VertexData {
		return &LookupVertexAdjIn{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else if ps.LastType == gdbi.EdgeData {
		return &LookupEdgeAdjIn{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else {
		return nil, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
	}
}

func (sc *DefaultStmtCompiler) InNull(stmt *gripql.GraphStatement_InNull, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.InNull)
	if ps.LastType == gdbi.VertexData {
		return &LookupVertexAdjIn{db: sc.db, labels: labels, loadData: ps.StepLoadData(), emitNull: true}, nil
	} else if ps.LastType == gdbi.EdgeData {
		return &LookupEdgeAdjIn{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else {
		return nil, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
	}
}
func (sc *DefaultStmtCompiler) Out(stmt *gripql.GraphStatement_Out, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.Out)
	if ps.LastType == gdbi.VertexData {
		return &LookupVertexAdjOut{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else if ps.LastType == gdbi.EdgeData {
		return &LookupEdgeAdjOut{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else {
		return nil, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
	}
}

func (sc *DefaultStmtCompiler) OutNull(stmt *gripql.GraphStatement_OutNull, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.OutNull)
	if ps.LastType == gdbi.VertexData {
		return &LookupVertexAdjOut{db: sc.db, labels: labels, loadData: ps.StepLoadData(), emitNull: true}, nil
	} else if ps.LastType == gdbi.EdgeData {
		return &LookupEdgeAdjOut{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
	} else {
		return nil, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
	}
}

func (sc *DefaultStmtCompiler) Both(stmt *gripql.GraphStatement_Both, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.Both)
	if ps.LastType == gdbi.VertexData {
		return &both{db: sc.db, labels: labels, lastType: gdbi.VertexData, toType: gdbi.VertexData, loadData: ps.StepLoadData()}, nil
	} else if ps.LastType == gdbi.EdgeData {
		return &both{db: sc.db, labels: labels, lastType: gdbi.EdgeData, toType: gdbi.VertexData, loadData: ps.StepLoadData()}, nil
	} else {
		return nil, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
	}
}

func (sc *DefaultStmtCompiler) InE(stmt *gripql.GraphStatement_InE, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.InE)
	return &InE{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
}

func (sc *DefaultStmtCompiler) InENull(stmt *gripql.GraphStatement_InENull, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.InENull)
	ps.LastType = gdbi.EdgeData
	return &InE{db: sc.db, labels: labels, loadData: ps.StepLoadData(), emitNull: true}, nil
}

func (sc *DefaultStmtCompiler) OutE(stmt *gripql.GraphStatement_OutE, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.OutE)
	ps.LastType = gdbi.EdgeData
	return &OutE{db: sc.db, labels: labels, loadData: ps.StepLoadData()}, nil
}

func (sc *DefaultStmtCompiler) OutENull(stmt *gripql.GraphStatement_OutENull, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.OutENull)
	ps.LastType = gdbi.EdgeData
	return &OutE{db: sc.db, labels: labels, loadData: ps.StepLoadData(), emitNull: true}, nil
}

func (sc *DefaultStmtCompiler) BothE(stmt *gripql.GraphStatement_BothE, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.BothE)
	ps.LastType = gdbi.EdgeData
	return &both{db: sc.db, labels: labels, lastType: gdbi.VertexData, toType: gdbi.EdgeData, loadData: ps.StepLoadData()}, nil
}

func (sc *DefaultStmtCompiler) Has(stmt *gripql.GraphStatement_Has, ps *gdbi.State) (gdbi.Processor, error) {
	return &Has{stmt.Has}, nil
}

func (sc *DefaultStmtCompiler) HasLabel(stmt *gripql.GraphStatement_HasLabel, ps *gdbi.State) (gdbi.Processor, error) {
	labels := protoutil.AsStringList(stmt.HasLabel)
	if len(labels) == 0 {
		return nil, fmt.Errorf(`no labels provided to "HasLabel" statement`)
	}
	return &HasLabel{labels}, nil
}

func (sc *DefaultStmtCompiler) HasKey(stmt *gripql.GraphStatement_HasKey, ps *gdbi.State) (gdbi.Processor, error) {
	keys := protoutil.AsStringList(stmt.HasKey)
	if len(keys) == 0 {
		return nil, fmt.Errorf(`no keys provided to "HasKey" statement`)
	}
	return &HasKey{keys}, nil
}

func (sc *DefaultStmtCompiler) HasID(stmt *gripql.GraphStatement_HasId, ps *gdbi.State) (gdbi.Processor, error) {
	ids := protoutil.AsStringList(stmt.HasId)
	if len(ids) == 0 {
		return nil, fmt.Errorf(`no ids provided to "HasId" statement`)
	}
	return &HasID{ids}, nil
}

func (sc *DefaultStmtCompiler) Limit(stmt *gripql.GraphStatement_Limit, ps *gdbi.State) (gdbi.Processor, error) {
	return &Limit{stmt.Limit}, nil
}

func (sc *DefaultStmtCompiler) Skip(stmt *gripql.GraphStatement_Skip, ps *gdbi.State) (gdbi.Processor, error) {
	return &Skip{stmt.Skip}, nil
}

func (sc *DefaultStmtCompiler) Range(stmt *gripql.GraphStatement_Range, ps *gdbi.State) (gdbi.Processor, error) {
	return &Range{start: stmt.Range.Start, stop: stmt.Range.Stop}, nil
}

func (sc *DefaultStmtCompiler) Count(stmt *gripql.GraphStatement_Count, ps *gdbi.State) (gdbi.Processor, error) {
	return &Count{}, nil
}

func (sc *DefaultStmtCompiler) Distinct(stmt *gripql.GraphStatement_Distinct, ps *gdbi.State) (gdbi.Processor, error) {
	fields := protoutil.AsStringList(stmt.Distinct)
	if len(fields) == 0 {
		fields = append(fields, "_gid")
	}
	return &Distinct{fields}, nil
}

func (sc *DefaultStmtCompiler) As(stmt *gripql.GraphStatement_As, ps *gdbi.State) (gdbi.Processor, error) {
	if stmt.As == "" {
		return nil, fmt.Errorf(`"mark" statement cannot have an empty name`)
	}
	if err := gripql.ValidateFieldName(stmt.As); err != nil {
		return nil, fmt.Errorf(`"mark" statement invalid; %v`, err)
	}
	if stmt.As == tpath.CURRENT {
		return nil, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, tpath.CURRENT)
	}
	ps.MarkTypes[stmt.As] = ps.LastType
	return &Marker{stmt.As}, nil
}

func (sc *DefaultStmtCompiler) Set(stmt *gripql.GraphStatement_Set, ps *gdbi.State) (gdbi.Processor, error) {
	return &ValueSet{key: stmt.Set.Key, value: stmt.Set.Value.AsInterface()}, nil
}

func (sc *DefaultStmtCompiler) Increment(stmt *gripql.GraphStatement_Increment, ps *gdbi.State) (gdbi.Processor, error) {
	return &ValueIncrement{key: stmt.Increment.Key, value: stmt.Increment.Value}, nil
}

func (sc *DefaultStmtCompiler) Mark(stmt *gripql.GraphStatement_Mark, ps *gdbi.State) (gdbi.Processor, error) {
	return &logic.JumpMark{Name: stmt.Mark}, nil
}

func (sc *DefaultStmtCompiler) Jump(stmt *gripql.GraphStatement_Jump, ps *gdbi.State) (gdbi.Processor, error) {
	return &logic.Jump{Mark: stmt.Jump.Mark, Stmt: stmt.Jump.Expression, Emit: stmt.Jump.Emit}, nil
}

func (sc *DefaultStmtCompiler) Select(stmt *gripql.GraphStatement_Select, ps *gdbi.State) (gdbi.Processor, error) {
	ps.LastType = ps.MarkTypes[stmt.Select]
	return &MarkSelect{stmt.Select}, nil
}

func (sc *DefaultStmtCompiler) Render(stmt *gripql.GraphStatement_Render, ps *gdbi.State) (gdbi.Processor, error) {
	return &Render{stmt.Render.AsInterface()}, nil
}

func (sc *DefaultStmtCompiler) Path(stmt *gripql.GraphStatement_Path, ps *gdbi.State) (gdbi.Processor, error) {
	return &Path{stmt.Path.AsSlice()}, nil
}

func (sc *DefaultStmtCompiler) Unwind(stmt *gripql.GraphStatement_Unwind, ps *gdbi.State) (gdbi.Processor, error) {
	return &Unwind{stmt.Unwind}, nil
}

func (sc *DefaultStmtCompiler) Fields(stmt *gripql.GraphStatement_Fields, ps *gdbi.State) (gdbi.Processor, error) {
	fields := protoutil.AsStringList(stmt.Fields)
	return &Fields{fields}, nil
}

func (sc *DefaultStmtCompiler) Aggregate(stmt *gripql.GraphStatement_Aggregate, ps *gdbi.State) (gdbi.Processor, error) {
	aggs := make(map[string]interface{})
	for _, a := range stmt.Aggregate.Aggregations {
		if _, ok := aggs[a.Name]; ok {
			return nil, fmt.Errorf("duplicate aggregation name '%s' found; all aggregations must have a unique name", a.Name)
		}
	}
	return &aggregate{stmt.Aggregate.Aggregations}, nil
}

func (sc *DefaultStmtCompiler) Custom(gs *gripql.GraphStatement, ps *gdbi.State) (gdbi.Processor, error) {

	switch stmt := gs.GetStatement().(type) {

	//Custom graph statements
	case *gripql.GraphStatement_LookupVertsIndex:
		ps.LastType = gdbi.VertexData
		return &LookupVertsIndex{db: sc.db, labels: stmt.Labels, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_EngineCustom:
		proc := stmt.Custom.(gdbi.CustomProcGen)
		ps.LastType = proc.GetType()
		return proc.GetProcessor(sc.db, ps)

	default:
		return nil, fmt.Errorf("grip compile: unknown statement type: %s", gs.GetStatement())
	}

}
