package engine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	//"log"
)

// Compile take set of statments and turns them into a runnable pipeline
func Compile(stmts []*aql.GraphStatement, db gdbi.GraphInterface) (Pipeline, error) {
	if len(stmts) == 0 {
		return Pipeline{}, nil
	}

	stmts = flatten(stmts)

	if err := validate(stmts); err != nil {
		return Pipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}
	rowTypes := []gdbi.DataType{}

	procs := make([]gdbi.Processor, 0, len(stmts))
	add := func(p gdbi.Processor) {
		procs = append(procs, p)
	}

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {

		case *aql.GraphStatement_V:
			ids := protoutil.AsStringList(stmt.V)
			add(&LookupVerts{db: db, ids: ids})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_E:
			var ids []string
			if stmt.E != "" {
				ids = append(ids, stmt.E)
			}
			add(&LookupEdges{db: db, ids: ids})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Has:
			add(&HasData{stmt.Has})

		case *aql.GraphStatement_HasLabel:
			labels := protoutil.AsStringList(stmt.HasLabel)
			add(&HasLabel{labels: labels})

		case *aql.GraphStatement_HasId:
			ids := protoutil.AsStringList(stmt.HasId)
			add(&HasID{ids: ids})

		case *aql.GraphStatement_In:
			labels := protoutil.AsStringList(stmt.In)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjIn{db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjIn{db, labels})
			} else {
				return Pipeline{}, fmt.Errorf(`"in" reached weird state`)
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Out:

			labels := protoutil.AsStringList(stmt.Out)
			if lastType == gdbi.VertexData {
				add(&LookupVertexAdjOut{db, labels})
			} else if lastType == gdbi.EdgeData {
				add(&LookupEdgeAdjOut{db, labels})
			} else {
				return Pipeline{}, fmt.Errorf(`"out" reached weird state`)
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Both:

			labels := protoutil.AsStringList(stmt.Both)
			if lastType == gdbi.VertexData {
				add(&concat{
					&LookupVertexAdjIn{db, labels},
					&LookupVertexAdjOut{db, labels},
				})
			} else if lastType == gdbi.EdgeData {
				add(&concat{
					&LookupEdgeAdjIn{db, labels},
					&LookupEdgeAdjOut{db, labels},
				})
			} else {
				return Pipeline{}, fmt.Errorf(`"both" reached weird state`)
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_InEdge:

			if lastType != gdbi.VertexData {
				return Pipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type`)
			}
			labels := protoutil.AsStringList(stmt.InEdge)
			add(&InEdge{db, labels})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_OutEdge:

			if lastType != gdbi.VertexData {
				return Pipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type`)
			}
			labels := protoutil.AsStringList(stmt.OutEdge)
			add(&OutEdge{db, labels})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_BothEdge:

			if lastType != gdbi.VertexData {
				return Pipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type`)
			}
			labels := protoutil.AsStringList(stmt.BothEdge)
			add(&concat{
				&InEdge{db, labels},
				&OutEdge{db, labels},
			})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Limit:
			add(&Limit{stmt.Limit})

		case *aql.GraphStatement_Count:
			// TODO validate the types following a counter
			add(&Count{})
			lastType = gdbi.CountData

		case *aql.GraphStatement_Fold:
			add(&Fold{stmt.Fold, []string{}})
			lastType = gdbi.ValueData

		case *aql.GraphStatement_GroupCount:
			// TODO validate the types following a counter
			add(&GroupCount{stmt.GroupCount})
			lastType = gdbi.GroupCountData

		case *aql.GraphStatement_As:
			// TODO probably needs to be checked for a lot of statements.
			if lastType == gdbi.NoData {
				return Pipeline{}, fmt.Errorf(`"as" statement is not valid at the beginning of a traversal`)
			}
			if stmt.As == "" {
				return Pipeline{}, fmt.Errorf(`"as" statement cannot have an empty name`)
			}
			markTypes[stmt.As] = lastType
			add(&Marker{stmt.As})

		case *aql.GraphStatement_Select:
			// TODO should track mark types so "lastType" can be set after select
			// TODO track mark names and fail when a name is missing.
			switch len(stmt.Select.Labels) {
			case 0:
				return Pipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			case 1:
				add(&selectOne{stmt.Select.Labels[0]})
				lastType = markTypes[stmt.Select.Labels[0]]
			default:
				add(&selectMany{stmt.Select.Labels})
				lastType = gdbi.RowData
				for _, i := range stmt.Select.Labels {
					rowTypes = append(rowTypes, markTypes[i])
				}
			}

		case *aql.GraphStatement_Values:
			add(&Values{stmt.Values.Labels})
			lastType = gdbi.ValueData

		/*
		   case *aql.GraphStatement_Import:
		   case *aql.GraphStatement_Map:
		   case *aql.GraphStatement_Fold:
		   case *aql.GraphStatement_Filter:
		   case *aql.GraphStatement_FilterValues:
		   case *aql.GraphStatement_VertexFromValues:
		*/

		default:
			return Pipeline{}, fmt.Errorf("unknown statement type")
		}
	}

	/*
	  dontLoad := true
	  for i := len(pipes) - 1; i >= 0; i-- {
	    switch p := pipes[i].(type) {
	    case *lookup, *lookupAdj, lookupEnd:
	      p.dontLoad = dontLoad
	      dontLoad = true
	    case *hasData:
	      dontLoad = false
	    case *count:
	      dontLoad = false
	    case *groupCount:
	      dontLoad = p.key == ""
	    }
	  }
	*/

	return Pipeline{procs, lastType, markTypes, rowTypes}, nil
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
