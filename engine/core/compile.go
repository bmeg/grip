package core

import (
	"fmt"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
)

// DefaultPipeline a set of runnable query operations
type DefaultPipeline struct {
	procs     []gdbi.Processor
	dataType  gdbi.DataType
	markTypes map[string]gdbi.DataType
}

func NewPipeline(procs []gdbi.Processor, ps *pipeline.State) *DefaultPipeline {
	return &DefaultPipeline{procs, ps.LastType, ps.MarkTypes}
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

// DefaultCompiler is the core compiler that works with default graph interface
type DefaultCompiler struct {
	db gdbi.GraphInterface
}

// NewCompiler creates a new compiler that runs using the provided GraphInterface
func NewCompiler(db gdbi.GraphInterface) gdbi.Compiler {
	return DefaultCompiler{db: db}
}

// Compile take set of statments and turns them into a runnable pipeline
func (comp DefaultCompiler) Compile(stmts []*gripql.GraphStatement) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &DefaultPipeline{}, nil
	}

	stmts = Flatten(stmts)

	if err := Validate(stmts); err != nil {
		return &DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	stmts = IndexStartOptimize(stmts)

	ps := pipeline.NewPipelineState(stmts)

	procs := make([]gdbi.Processor, 0, len(stmts))

	for i, gs := range stmts {
		ps.SetCurStatment(i)
		p, err := StatementProcessor(gs, comp.db, ps)
		if err != nil {
			return &DefaultPipeline{}, err
		}
		procs = append(procs, p)
	}

	return &DefaultPipeline{procs, ps.LastType, ps.MarkTypes}, nil
}

func StatementProcessor(gs *gripql.GraphStatement, db gdbi.GraphInterface, ps *pipeline.State) (gdbi.Processor, error) {
	switch stmt := gs.GetStatement().(type) {

	case *gripql.GraphStatement_V:
		if ps.LastType != gdbi.NoData {
			return nil, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
		}
		ids := protoutil.AsStringList(stmt.V)
		ps.LastType = gdbi.VertexData
		return &LookupVerts{db: db, ids: ids, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_E:
		if ps.LastType != gdbi.NoData {
			return nil, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
		}
		ids := protoutil.AsStringList(stmt.E)
		ps.LastType = gdbi.EdgeData
		return &LookupEdges{db: db, ids: ids, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_Search:
		if ps.LastType != gdbi.NoData {
			return nil, fmt.Errorf(`"Index" statement is only valid at the beginning of the traversal`)
		}
		ps.LastType = gdbi.VertexData
		return &LookupIndex{db: db, query: stmt.Search, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_In, *gripql.GraphStatement_InV:
		labels := append(protoutil.AsStringList(gs.GetIn()), protoutil.AsStringList(gs.GetInV())...)
		if ps.LastType == gdbi.VertexData {
			ps.LastType = gdbi.VertexData
			return &LookupVertexAdjIn{db: db, labels: labels, loadData: ps.StepLoadData()}, nil
		} else if ps.LastType == gdbi.EdgeData {
			ps.LastType = gdbi.VertexData
			return &LookupEdgeAdjIn{db: db, labels: labels, loadData: ps.StepLoadData()}, nil
		} else {
			return nil, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}

	case *gripql.GraphStatement_Out, *gripql.GraphStatement_OutV:
		labels := append(protoutil.AsStringList(gs.GetOut()), protoutil.AsStringList(gs.GetOutV())...)
		if ps.LastType == gdbi.VertexData {
			ps.LastType = gdbi.VertexData
			return &LookupVertexAdjOut{db: db, labels: labels, loadData: ps.StepLoadData()}, nil
		} else if ps.LastType == gdbi.EdgeData {
			ps.LastType = gdbi.VertexData
			return &LookupEdgeAdjOut{db: db, labels: labels, loadData: ps.StepLoadData()}, nil
		} else {
			return nil, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}

	case *gripql.GraphStatement_Both, *gripql.GraphStatement_BothV:
		labels := append(protoutil.AsStringList(gs.GetBoth()), protoutil.AsStringList(gs.GetBothV())...)
		if ps.LastType == gdbi.VertexData {
			ps.LastType = gdbi.VertexData
			return &both{db: db, labels: labels, lastType: gdbi.VertexData, toType: gdbi.VertexData, loadData: ps.StepLoadData()}, nil
		} else if ps.LastType == gdbi.EdgeData {
			ps.LastType = gdbi.VertexData
			return &both{db: db, labels: labels, lastType: gdbi.EdgeData, toType: gdbi.VertexData, loadData: ps.StepLoadData()}, nil
		} else {
			return nil, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}

	case *gripql.GraphStatement_InE:
		if ps.LastType != gdbi.VertexData {
			return nil, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		labels := protoutil.AsStringList(stmt.InE)
		ps.LastType = gdbi.EdgeData
		return &InE{db: db, labels: labels, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_OutE:
		if ps.LastType != gdbi.VertexData {
			return nil, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		labels := protoutil.AsStringList(stmt.OutE)
		ps.LastType = gdbi.EdgeData
		return &OutE{db: db, labels: labels, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_BothE:
		if ps.LastType != gdbi.VertexData {
			return nil, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		labels := protoutil.AsStringList(stmt.BothE)
		ps.LastType = gdbi.EdgeData
		return &both{db: db, labels: labels, lastType: gdbi.VertexData, toType: gdbi.EdgeData, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_Has:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"Has" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		return &Has{stmt.Has}, nil

	case *gripql.GraphStatement_HasLabel:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"HasLabel" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		labels := protoutil.AsStringList(stmt.HasLabel)
		if len(labels) == 0 {
			return nil, fmt.Errorf(`no labels provided to "HasLabel" statement`)
		}
		return &HasLabel{labels}, nil

	case *gripql.GraphStatement_HasKey:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"HasKey" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		keys := protoutil.AsStringList(stmt.HasKey)
		if len(keys) == 0 {
			return nil, fmt.Errorf(`no keys provided to "HasKey" statement`)
		}
		return &HasKey{keys}, nil

	case *gripql.GraphStatement_HasId:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"HasId" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		ids := protoutil.AsStringList(stmt.HasId)
		if len(ids) == 0 {
			return nil, fmt.Errorf(`no ids provided to "HasId" statement`)
		}
		return &HasID{ids}, nil

	case *gripql.GraphStatement_Limit:
		return &Limit{stmt.Limit}, nil

	case *gripql.GraphStatement_Skip:
		return &Skip{stmt.Skip}, nil

	case *gripql.GraphStatement_Range:
		return &Range{start: stmt.Range.Start, stop: stmt.Range.Stop}, nil

	case *gripql.GraphStatement_Count:
		ps.LastType = gdbi.CountData
		return &Count{}, nil

	case *gripql.GraphStatement_Distinct:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		fields := protoutil.AsStringList(stmt.Distinct)
		if len(fields) == 0 {
			fields = append(fields, "_gid")
		}
		return &Distinct{fields}, nil

	case *gripql.GraphStatement_As:
		if ps.LastType == gdbi.NoData {
			return nil, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
		}
		if stmt.As == "" {
			return nil, fmt.Errorf(`"mark" statement cannot have an empty name`)
		}
		if err := gripql.ValidateFieldName(stmt.As); err != nil {
			return nil, fmt.Errorf(`"mark" statement invalid; %v`, err)
		}
		if stmt.As == jsonpath.Current {
			return nil, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
		}
		ps.MarkTypes[stmt.As] = ps.LastType
		return &Marker{stmt.As}, nil

	case *gripql.GraphStatement_Select:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		switch len(stmt.Select.Marks) {
		case 0:
			return nil, fmt.Errorf(`"select" statement has an empty list of mark names`)
		case 1:
			ps.LastType = ps.MarkTypes[stmt.Select.Marks[0]]
			return &Jump{stmt.Select.Marks[0]}, nil
		default:
			ps.LastType = gdbi.SelectionData
			return &Selector{stmt.Select.Marks}, nil
		}

	case *gripql.GraphStatement_Render:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		ps.LastType = gdbi.RenderData
		return &Render{protoutil.UnWrapValue(stmt.Render)}, nil

	case *gripql.GraphStatement_Fields:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		fields := protoutil.AsStringList(stmt.Fields)
		return &Fields{fields}, nil

	case *gripql.GraphStatement_Aggregate:
		if ps.LastType != gdbi.VertexData && ps.LastType != gdbi.EdgeData {
			return nil, fmt.Errorf(`"aggregate" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		aggs := make(map[string]interface{})
		for _, a := range stmt.Aggregate.Aggregations {
			if _, ok := aggs[a.Name]; ok {
				return nil, fmt.Errorf("duplicate aggregation name '%s' found; all aggregations must have a unique name", a.Name)
			}
		}
		ps.LastType = gdbi.AggregationData
		return &aggregate{stmt.Aggregate.Aggregations}, nil

	//Custom graph statements
	case *gripql.GraphStatement_LookupVertsIndex:
		ps.LastType = gdbi.VertexData
		return &LookupVertsIndex{db: db, labels: stmt.Labels, loadData: ps.StepLoadData()}, nil

	case *gripql.GraphStatement_EngineCustom:
		proc := stmt.Custom.(gdbi.CustomProcGen)
		return proc.GetProcessor(db, ps)

	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}

//Validate checks pipeline for chains of statements that won't work
func Validate(stmts []*gripql.GraphStatement) error {
	for i, gs := range stmts {
		// Validate that the first statement is V() or E()
		if i == 0 {
			switch gs.GetStatement().(type) {
			case *gripql.GraphStatement_V, *gripql.GraphStatement_E, *gripql.GraphStatement_Search:
			default:
				return fmt.Errorf("first statement is not V(), E() or Search(): %s", gs)
			}
		}
	}
	return nil
}

// Flatten flattens Match statements
func Flatten(stmts []*gripql.GraphStatement) []*gripql.GraphStatement {
	out := make([]*gripql.GraphStatement, 0, len(stmts))
	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *gripql.GraphStatement_Match:
			for _, q := range stmt.Match.Queries {
				out = append(out, Flatten(q.Query)...)
			}
		default:
			out = append(out, gs)
		}
	}
	return out
}
