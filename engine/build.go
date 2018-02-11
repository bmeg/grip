package engine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

// TODO with a query planner, "load" could instead be a list of property keys

// TODO pre-process query? or process processors?
// - need internal processors to exist because they hold the fields
//   needed to collapse two steps.
  // TODO rules needed
  // - V().HasID("id1") from allV + idFilter -> lookupByID
  // - V().Has("foo", "1").Has("foo", "2")
  // - V().HasLabel("foo").HasID("1").Has("foo", "1").Has("bar", "2")
  // - V().HasLabel("foo").out("bar").Has("afoo", 1).Has("bar", 2).HasLabel("baz")
  // - V().E()
  // - V().HasID().HasID()
  // - V().out("foo").V()
  // - V().out("foo").E()
  // - V().HasLabel("foo").in()
  // - V().HasLabel("foo").outE("some-edge").has("foo", "bar").outV()
  //     - should optimize edge scan to look for matching data properties, if possible? or is that too much trouble?

func run() {
  db.View transaction
}

func statementPipes(db DBI, stmts []*aql.GraphStatement) ([]pipe, error) {
  last := None
  pipes := make([]pipe, 0, len(stmts))
  add := func(p pipe) {
    pipes = append(pipes, p)
  }

  for _, gs := range stmts {
    switch stmt := gs.Statement.(type) {

    case *aql.GraphStatement_V:
      ids := protoutil.AsStringList(stmt.V)
      add(&lookup{db: db, ids: ids, elType: Vertex})
      last = Vertex

    case *aql.GraphStatement_E:
      ids := protoutil.AsStringList(stmt.E)
      add(&lookup{db: db, ids: ids, elType: Edge})
      last = Vertex

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
      if last != Vertex {
        return nil, fmt.Errorf(`"in" is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.In)
      add(&lookupAdj{db, in, labels})

    case *aql.GraphStatement_Out:

      if last != Vertex {
        // TODO need inV, outV, bothV
        // TODO what does ophion do?
        // TODO can coerce out() to accept edges? what does "labels" mean?
        //      vertex label?
        return nil, fmt.Errorf(`"out" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.Out)
      add(&lookupAdj{db, out, labels})

    case *aql.GraphStatement_Both:

      if last != Vertex {
        return nil, fmt.Errorf(`"both" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.Both)
      add(&lookupAdj{db, both, labels})

    case *aql.GraphStatement_InEdge:

      if last != Vertex {
        return nil, fmt.Errorf(`"inEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.InEdge)
      add(&lookupEnd{db, in, labels})
      last = Edge

    case *aql.GraphStatement_OutEdge:

      if last != Vertex {
        return nil, fmt.Errorf(`"outEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.OutEdge)
      add(&lookupEnd{db, out, labels})
      last = Edge

    case *aql.GraphStatement_BothEdge:

      if last != Vertex {
        return nil, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type`)
      }
      labels := protoutil.AsStringList(stmt.BothEdge)
      add(&lookupEnd{db, both, labels})
      last = Edge

    case *aql.GraphStatement_Limit:
      add(&limit{stmt.Limit})

    case *aql.GraphStatement_Count:
      // TODO validate the types following a counter
      add(&count{})
      last = Count

    /*
    case *aql.GraphStatement_As:
    case *aql.GraphStatement_Select:
    case *aql.GraphStatement_Values:
    case *aql.GraphStatement_GroupCount:
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
  return pipes, nil
}
