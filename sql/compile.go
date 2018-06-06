package sql

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
)

// Compiler is a SQL database specific compiler that works with default graph interface
type Compiler struct{}

// NewCompiler creates a new compiler that runs using the provided GraphInterface
func NewCompiler() gdbi.Compiler {
	return &Compiler{}
}

// Compile compiles a set of graph traversal statements into a mongo aggregation pipeline
func (comp *Compiler) Compile(stmts []*aql.GraphStatement) (gdbi.Pipeline, error) {
	procs := []gdbi.Processor{}
	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}

	stmts = core.Flatten(stmts)

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *aql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_In:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Out:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Both:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_InEdge:
			if lastType != gdbi.VertexData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_OutEdge:
			if lastType != gdbi.VertexData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_BothEdge:
			if lastType != gdbi.VertexData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Where:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"where" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}

		case *aql.GraphStatement_Limit:

		case *aql.GraphStatement_Offset:

		case *aql.GraphStatement_Count:
			lastType = gdbi.CountData

		case *aql.GraphStatement_Distinct:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}

		case *aql.GraphStatement_Mark:
			if lastType == gdbi.NoData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
			}
			if stmt.Mark == "" {
				return &core.DefaultPipeline{}, fmt.Errorf(`"mark" statement cannot have an empty name`)
			}
			if err := aql.ValidateFieldName(stmt.Mark); err != nil {
				return &core.DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; %v`, err)
			}
			if stmt.Mark == jsonpath.Current {
				return &core.DefaultPipeline{}, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.Mark] = lastType

		case *aql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			if len(stmt.Select.Marks) == 0 {
				return &core.DefaultPipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			}
			lastType = gdbi.SelectionData

		case *aql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			procs = append(procs, &core.Render{Template: protoutil.UnWrapValue(stmt.Render)})
			lastType = gdbi.RenderData

		case *aql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}

		case *aql.GraphStatement_Aggregate:
			if lastType != gdbi.VertexData {
				return &core.DefaultPipeline{}, fmt.Errorf(`"aggregate" statement is only valid for vertex types not: %s`, lastType.String())
			}
			aggNames := make(map[string]interface{})
			for _, a := range stmt.Aggregate.Aggregations {
				if _, ok := aggNames[a.Name]; ok {
					return &core.DefaultPipeline{}, fmt.Errorf("duplicate aggregation name '%s' found; all aggregations must have a unique name", a.Name)
				}
			}
			lastType = gdbi.AggregationData

		default:
			return &core.DefaultPipeline{}, fmt.Errorf("unknown statement type")
		}
	}

	return &core.DefaultPipeline{}, nil
}
