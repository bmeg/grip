package engine

import (
  "context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

type DB interface {
  GetVertexList(context.Context, bool) <-chan *aql.Vertex
  GetEdgeList(context.Context, bool) <-chan *aql.Edge
  GetInList(context.Context, string, bool) <-chan *aql.Vertex
  GetOutList(context.Context, string, bool) <-chan *aql.Vertex
	GetOutEdgeList(context.Context, string, bool) <-chan *aql.Edge
	GetInEdgeList(context.Context, string, bool) <-chan *aql.Edge
}

func compile(db DB, stmts []*aql.GraphStatement) ([]processor, error) {
  if len(stmts) == 0 {
    return nil, nil
  }

  last := noData
  procs := make([]processor, 0, len(stmts))
  add := func(p processor) {
    procs = append(procs, p)
  }

  for i, gs := range stmts {
    // Validate that the first statement is V() or E() 
    if i == 0 {
      switch gs.GetStatement().(type) {
      case *aql.GraphStatement_V, *aql.GraphStatement_E:
      default:
        return nil, fmt.Errorf("first statement is not V() or E()")
      }
    }

    switch stmt := gs.GetStatement().(type) {

    case *aql.GraphStatement_V:
      ids := protoutil.AsStringList(stmt.V)
      add(&lookupVerts{db: db, ids: ids})
      last = vertexData

    case *aql.GraphStatement_E:
      ids := protoutil.AsStringList(stmt.E)
      add(&lookupEdges{db: db, ids: ids})
      last = edgeData

    case *aql.GraphStatement_Has:
      add(&hasData{stmt.Has})

    case *aql.GraphStatement_HasLabel:
      labels := protoutil.AsStringList(stmt.HasLabel)
      add(&hasLabel{labels: labels})

    case *aql.GraphStatement_HasId:
      ids := protoutil.AsStringList(stmt.HasId)
      add(&hasID{ids: ids})

    case *aql.GraphStatement_In:
      // TODO should validation happen in a pre-processing step?
      //      there may end up being too many rules to fit here.
      if last != vertexData {
        return nil, fmt.Errorf(`"in" is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.In)
      add(&lookupAdjIn{db, labels})

    case *aql.GraphStatement_Out:

      if last != vertexData {
        // TODO need inV, outV, bothV
        // TODO what does ophion do?
        // TODO can coerce out() to accept edges? what does "labels" mean?
        //      vertex label?
        return nil, fmt.Errorf(`"out" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.Out)
      add(&lookupAdjOut{db, labels})

    case *aql.GraphStatement_Both:

      if last != vertexData {
        return nil, fmt.Errorf(`"both" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.Both)
      add(&concat{
        &lookupAdjIn{db, labels},
        &lookupAdjOut{db, labels},
      })

    case *aql.GraphStatement_InEdge:

      if last != vertexData {
        return nil, fmt.Errorf(`"inEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.InEdge)
      add(&inEdge{db, labels})
      last = edgeData

    case *aql.GraphStatement_OutEdge:

      if last != vertexData {
        return nil, fmt.Errorf(`"outEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.OutEdge)
      add(&outEdge{db, labels})
      last = edgeData

    case *aql.GraphStatement_BothEdge:

      if last != vertexData {
        return nil, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.BothEdge)
      add(&concat{
        &inEdge{db, labels},
        &outEdge{db, labels},
      })
      last = edgeData

    case *aql.GraphStatement_Limit:
      add(&limit{stmt.Limit})

    case *aql.GraphStatement_Count:
      // TODO validate the types following a counter
      add(&count{})
      last = countData

    case *aql.GraphStatement_GroupCount:
      // TODO validate the types following a counter
      add(&groupCount{stmt.GroupCount})
      last = groupCountData

    case *aql.GraphStatement_As:
      // TODO probably needs to be checked for a lot of statements.
      if last == noData {
        return nil, fmt.Errorf(`"as" statement is not valid at the beginning of a traversal`)
      }
      if stmt.As == "" {
        return nil, fmt.Errorf(`"as" statement cannot have an empty name`)
      }
      // TODO support multiple keys in aql
      marks := []string{stmt.As}
      add(&marker{marks})

    case *aql.GraphStatement_Select:
      // TODO should track mark types so "last" can be set after select
      // TODO track mark names and fail when a name is missing.
      switch len(stmt.Select.Labels) {
      case 0:
        return nil, fmt.Errorf(`"select" statement has an empty list of mark names`)
      case 1:
        add(&selectOne{stmt.Select.Labels[0]})
      default:
        add(&selectMany{stmt.Select.Labels})
        last = rowData
      }

    case *aql.GraphStatement_Values:
      add(&values{stmt.Values.Labels})
      last = valueData

    /*
    case *aql.GraphStatement_Match:

    case *aql.GraphStatement_Import:
    case *aql.GraphStatement_Map:
    case *aql.GraphStatement_Fold:
    case *aql.GraphStatement_Filter:
    case *aql.GraphStatement_FilterValues:
    case *aql.GraphStatement_VertexFromValues:
    */

    default:
      return nil, fmt.Errorf("unknown statement type")
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

  return procs, nil
}
