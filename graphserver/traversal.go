
package graphserver

import (
  "fmt"
  "log"
  "context"
  "github.com/bmeg/arachne/protoutil"
  "github.com/bmeg/arachne/gdbi"
  "github.com/bmeg/arachne/aql"
)

// Traversal is a compiled aql.GraphQuery connected to a ArachneInterface
type Traversal struct {
	graph  string
	engine *GraphEngine
	Query  gdbi.QueryInterface
}

// SubQuery initializes a new sub-traversal for a traversal
func (trav *Traversal) SubQuery() *Traversal {
	return &Traversal{Query: trav.engine.Arachne.Query(trav.graph), engine: trav.engine, graph: trav.graph}
}

// UnpackQuery takes a aql.GraphQuery subquery (ie a traversal run as a child element
// of another traversal) and creates a valid Traversal
func UnpackQuery(query *aql.GraphQuery, tr *Traversal) (*Traversal, error) {
	for _, s := range query.Query {
		err := tr.AddStatement(s)
		if err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
	}
	return tr, nil
}

// RunStatement adds on more query statement to a traversal
func (trav *Traversal) AddStatement(statement *aql.GraphStatement) error {
	if x, ok := statement.GetStatement().(*aql.GraphStatement_V); ok {
		vlist := protoutil.AsStringList(x.V)
		trav.Query = trav.Query.V(vlist)
	} else if _, ok := statement.GetStatement().(*aql.GraphStatement_E); ok {
		trav.Query = trav.Query.E()
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Out); ok {
		labels := protoutil.AsStringList(x.Out)
		trav.Query = trav.Query.Out(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_In); ok {
		labels := protoutil.AsStringList(x.In)
		trav.Query = trav.Query.In(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Both); ok {
		labels := protoutil.AsStringList(x.Both)
		trav.Query = trav.Query.Both(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_OutEdge); ok {
		labels := protoutil.AsStringList(x.OutEdge)
		trav.Query = trav.Query.OutE(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_InEdge); ok {
		labels := protoutil.AsStringList(x.InEdge)
		trav.Query = trav.Query.InE(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_BothEdge); ok {
		labels := protoutil.AsStringList(x.BothEdge)
		trav.Query = trav.Query.BothE(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_OutBundle); ok {
		labels := protoutil.AsStringList(x.OutBundle)
		trav.Query = trav.Query.OutBundle(labels...)
	} else if x := statement.GetHas(); x != nil {
		trav.Query = trav.Query.Has(x.Key, x.Within...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_HasLabel); ok {
		labels := protoutil.AsStringList(x.HasLabel)
		trav.Query = trav.Query.HasLabel(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_HasId); ok {
		ids := protoutil.AsStringList(x.HasId)
		trav.Query = trav.Query.HasID(ids...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Limit); ok {
		trav.Query = trav.Query.Limit(x.Limit)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Values); ok {
		trav.Query = trav.Query.Values(x.Values.Labels)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Import); ok {
		trav.Query = trav.Query.Import(x.Import)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Map); ok {
		trav.Query = trav.Query.Map(x.Map)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Fold); ok {
		trav.Query = trav.Query.Fold(x.Fold.Source, x.Fold.Init)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Filter); ok {
		trav.Query = trav.Query.Filter(x.Filter)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_FilterValues); ok {
		trav.Query = trav.Query.FilterValues(x.FilterValues)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_VertexFromValues); ok {
		trav.Query = trav.Query.VertexFromValues(x.VertexFromValues)
	} else if _, ok := statement.GetStatement().(*aql.GraphStatement_Count); ok {
		trav.Query = trav.Query.Count()
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_As); ok {
		trav.Query = trav.Query.As(x.As)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Select); ok {
		trav.Query = trav.Query.Select(x.Select.Labels)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_GroupCount); ok {
		trav.Query = trav.Query.GroupCount(x.GroupCount)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Match); ok {
		matches := []*gdbi.QueryInterface{}
		for _, q := range x.Match.Queries {
			tr := trav.SubQuery()
			subtr, err := UnpackQuery(q, tr)
			if err != nil {
				return err
			}
			matches = append(matches, &subtr.Query)
		}
		trav.Query = trav.Query.Match(matches)
	} else {
		log.Printf("Unknown Statement: %#v", statement)
		return fmt.Errorf("Unknown Statement: %#v", statement)
	}
	return nil
}

// GetResult executes the Traversal
func (trav *Traversal) GetResult(ctx context.Context) (chan aql.ResultRow, error) {
	e := trav.Query.Execute(ctx)
	if e == nil {
		return nil, fmt.Errorf("Query Failed")
	}
	return e, nil
}
