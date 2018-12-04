package core

import (
	"fmt"

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

	if err := validate(stmts); err != nil {
		return &DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}

	procs := make([]gdbi.Processor, 0, len(stmts))
	add := func(p gdbi.Processor) {
		procs = append(procs, p)
	}

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {

		case *gripql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			ids := protoutil.AsStringList(stmt.V)
			add(&LookupVerts{db: comp.db, ids: ids})
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			ids := protoutil.AsStringList(stmt.E)
			add(&LookupEdges{db: comp.db, ids: ids})
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_In, *gripql.GraphStatement_InV:
			labels := append(protoutil.AsStringList(gs.GetIn()), protoutil.AsStringList(gs.GetInV())...)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjIn{comp.db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjIn{comp.db, labels})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_Out, *gripql.GraphStatement_OutV:
			labels := append(protoutil.AsStringList(gs.GetOut()), protoutil.AsStringList(gs.GetOutV())...)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjOut{comp.db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjOut{comp.db, labels})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_Both, *gripql.GraphStatement_BothV:
			labels := append(protoutil.AsStringList(gs.GetBoth()), protoutil.AsStringList(gs.GetBothV())...)
			if lastType == gdbi.VertexData {
				add(&both{comp.db, labels, gdbi.VertexData, gdbi.VertexData})
			} else if lastType == gdbi.EdgeData {
				add(&both{comp.db, labels, gdbi.EdgeData, gdbi.VertexData})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_InE:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.InE)
			add(&InE{comp.db, labels})
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_OutE:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.OutE)
			add(&OutE{comp.db, labels})
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_BothE:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.BothE)
			add(&both{comp.db, labels, gdbi.VertexData, gdbi.EdgeData})
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_Has:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"Has" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			add(&Has{stmt.Has})

		case *gripql.GraphStatement_HasLabel:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"HasLabel" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.HasLabel)
			if len(labels) == 0 {
				return &DefaultPipeline{}, fmt.Errorf(`no labels provided to "HasLabel" statement`)
			}
			add(&HasLabel{labels})

		case *gripql.GraphStatement_HasKey:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"HasKey" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			keys := protoutil.AsStringList(stmt.HasKey)
			if len(keys) == 0 {
				return &DefaultPipeline{}, fmt.Errorf(`no keys provided to "HasKey" statement`)
			}
			add(&HasKey{keys})

		case *gripql.GraphStatement_HasId:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"HasId" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			ids := protoutil.AsStringList(stmt.HasId)
			if len(ids) == 0 {
				return &DefaultPipeline{}, fmt.Errorf(`no ids provided to "HasId" statement`)
			}
			add(&HasID{ids})

		case *gripql.GraphStatement_Limit:
			add(&Limit{stmt.Limit})

		case *gripql.GraphStatement_Skip:
			add(&Skip{stmt.Skip})

		case *gripql.GraphStatement_Range:
			add(&Range{start: stmt.Range.Start, stop: stmt.Range.Stop})

		case *gripql.GraphStatement_Count:
			add(&Count{})
			lastType = gdbi.CountData

		case *gripql.GraphStatement_Distinct:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Distinct)
			if len(fields) == 0 {
				fields = append(fields, "_gid")
			}
			add(&Distinct{fields})

		case *gripql.GraphStatement_As:
			if lastType == gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
			}
			if stmt.As == "" {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement cannot have an empty name`)
			}
			if err := gripql.ValidateFieldName(stmt.As); err != nil {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; %v`, err)
			}
			if stmt.As == jsonpath.Current {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.As] = lastType
			add(&Marker{stmt.As})

		case *gripql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			switch len(stmt.Select.Marks) {
			case 0:
				return &DefaultPipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			case 1:
				add(&Jump{stmt.Select.Marks[0]})
				lastType = markTypes[stmt.Select.Marks[0]]
			default:
				add(&Selector{stmt.Select.Marks})
				lastType = gdbi.SelectionData
			}

		case *gripql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			add(&Render{protoutil.UnWrapValue(stmt.Render)})
			lastType = gdbi.RenderData

		case *gripql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Fields)
			add(&Fields{fields})

		case *gripql.GraphStatement_Aggregate:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"aggregate" statement is only valid for vertex types not: %s`, lastType.String())
			}
			aggs := make(map[string]interface{})
			for _, a := range stmt.Aggregate.Aggregations {
				if _, ok := aggs[a.Name]; ok {
					return &DefaultPipeline{}, fmt.Errorf("duplicate aggregation name '%s' found; all aggregations must have a unique name", a.Name)
				}
			}
			add(&aggregate{stmt.Aggregate.Aggregations})
			lastType = gdbi.AggregationData

		default:
			return &DefaultPipeline{}, fmt.Errorf("unknown statement type")
		}
	}

	procs = indexStartOptimize(procs)

	return &DefaultPipeline{procs, lastType, markTypes}, nil
}

// For V().Has(Eq("$.label", "Person")) and V().Has(Eq("$.gid", "1")) queries, streamline into a single index lookup
func indexStartOptimize(pipe []gdbi.Processor) []gdbi.Processor {
	if len(pipe) >= 2 {
		if lookupV, ok := pipe[0].(*LookupVerts); ok {
			if len(lookupV.ids) == 0 {
				if where, ok := pipe[1].(*Has); ok {
					if cond := where.stmt.GetCondition(); cond != nil {
						vals := []string{}
						path := jsonpath.GetJSONPath(cond.Key)
						if path == "$.label" || path == "$.gid" {
							val := protoutil.UnWrapValue(cond.Value)
							switch cond.Condition {
							case gripql.Condition_EQ:
								if l, ok := val.(string); ok {
									vals = []string{l}
								}
							case gripql.Condition_WITHIN:
								if l, ok := val.([]string); ok {
									vals = l
								}
							default:
								// do nothing
							}
						}
						if len(vals) > 0 {
							if path == "$.label" {
								hIdx := LookupVertsIndex{labels: vals, db: lookupV.db}
								return append([]gdbi.Processor{&hIdx}, pipe[2:]...)
							} else if path == "$.gid" {
								hIdx := LookupVerts{ids: vals, db: lookupV.db}
								return append([]gdbi.Processor{&hIdx}, pipe[2:]...)
							}
						}
					}
				}
			}
		}
	}
	return pipe
}

func validate(stmts []*gripql.GraphStatement) error {
	for i, gs := range stmts {
		// Validate that the first statement is V() or E()
		if i == 0 {
			switch gs.GetStatement().(type) {
			case *gripql.GraphStatement_V, *gripql.GraphStatement_E:
			default:
				return fmt.Errorf("first statement is not V() or E(): %s", gs)
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
