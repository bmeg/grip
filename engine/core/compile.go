package core

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
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
func (comp DefaultCompiler) Compile(stmts []*aql.GraphStatement) (gdbi.Pipeline, error) {
	if len(stmts) == 0 {
		return &DefaultPipeline{}, nil
	}

	stmts = flatten(stmts)

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

		case *aql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			ids := protoutil.AsStringList(stmt.V)
			add(&LookupVerts{db: comp.db, ids: ids})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			ids := protoutil.AsStringList(stmt.E)
			add(&LookupEdges{db: comp.db, ids: ids})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_In:
			labels := protoutil.AsStringList(stmt.In)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjIn{comp.db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjIn{comp.db, labels})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Out:
			labels := protoutil.AsStringList(stmt.Out)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjOut{comp.db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjOut{comp.db, labels})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Both:
			labels := protoutil.AsStringList(stmt.Both)
			if lastType == gdbi.VertexData {
				add(&concat{
					&LookupVertexAdjIn{comp.db, labels},
					&LookupVertexAdjOut{comp.db, labels},
				})
			} else if lastType == gdbi.EdgeData {
				add(&concat{
					&LookupEdgeAdjIn{comp.db, labels},
					&LookupEdgeAdjOut{comp.db, labels},
				})
			} else {
				return &DefaultPipeline{}, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_InEdge:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.InEdge)
			add(&InEdge{comp.db, labels})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_OutEdge:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.OutEdge)
			add(&OutEdge{comp.db, labels})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_BothEdge:
			if lastType != gdbi.VertexData {
				return &DefaultPipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.BothEdge)
			add(&concat{
				&InEdge{comp.db, labels},
				&OutEdge{comp.db, labels},
			})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Where:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			add(&Where{stmt.Where})

		case *aql.GraphStatement_Limit:
			add(&Limit{stmt.Limit})

		case *aql.GraphStatement_Count:
			// TODO validate the types following a counter
			add(&Count{})
			lastType = gdbi.CountData

		case *aql.GraphStatement_Distinct:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			add(&Distinct{protoutil.AsStringList(stmt.Distinct)})

		case *aql.GraphStatement_Mark:
			// TODO probably needs to be checked for a lot of statements.
			if lastType == gdbi.NoData {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
			}
			if stmt.Mark == "" {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement cannot have an empty name`)
			}
			if err := aql.ValidateFieldName(stmt.Mark); err != nil {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; %v`, err)
			}
			if stmt.Mark == jsonpath.Current {
				return &DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.Mark] = lastType
			add(&Marker{stmt.Mark})

		case *aql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			// TODO should track mark types so "lastType" can be set after select
			// TODO track mark names and fail when a name is missing.
			if len(stmt.Select.Marks) == 0 {
				return &DefaultPipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			}
			lastType = gdbi.SelectionData
			add(&Selector{stmt.Select.Marks})

		case *aql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			r := Render{protoutil.UnWrapValue(stmt.Render)}
			add(&r)
			lastType = gdbi.RenderData

		case *aql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &DefaultPipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Fields)
			add(&Fields{fields})

		case *aql.GraphStatement_Aggregate:
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

// For V().Where(Eq("$.label", "Person")) and V().Where(Eq("$.gid", "1")) queries, streamline into a single index lookup
func indexStartOptimize(pipe []gdbi.Processor) []gdbi.Processor {
	if len(pipe) >= 2 {
		if lookupV, ok := pipe[0].(*LookupVerts); ok {
			if len(lookupV.ids) == 0 {
				if where, ok := pipe[1].(*Where); ok {
					if cond := where.stmt.GetCondition(); cond != nil {
						vals := []string{}
						path := jsonpath.GetJSONPath(cond.Key)
						if path == "$.label" || path == "$.gid" {
							val := protoutil.UnWrapValue(cond.Value)
							switch cond.Condition {
							case aql.Condition_EQ:
								if l, ok := val.(string); ok {
									vals = []string{l}
								}
							case aql.Condition_IN:
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

func validate(stmts []*aql.GraphStatement) error {
	for i, gs := range stmts {
		// Validate that the first statement is V() or E()
		if i == 0 {
			switch gs.GetStatement().(type) {
			case *aql.GraphStatement_V, *aql.GraphStatement_E:
			default:
				return fmt.Errorf("first statement is not V() or E(): %s", gs)
			}
		}
	}
	return nil
}

func flatten(stmts []*aql.GraphStatement) []*aql.GraphStatement {
	out := make([]*aql.GraphStatement, 0, len(stmts))
	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *aql.GraphStatement_Match:
			for _, q := range stmt.Match.Queries {
				out = append(out, flatten(q.Query)...)
			}
		default:
			out = append(out, gs)
		}
	}
	return out
}
