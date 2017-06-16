package graphserver

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"log"
)

type GraphEngine struct {
	Arachne gdbi.ArachneInterface
}

func NewGraphEngine(a gdbi.ArachneInterface) GraphEngine {
	return GraphEngine{Arachne: a}
}

func (engine *GraphEngine) GetGraphs() []string {
	return engine.Arachne.GetGraphs()
}

func (engine *GraphEngine) GetVertex(graph, id string) *aql.Vertex {
	return engine.Arachne.Graph(graph).GetVertex(id, true)
}

func (engine *GraphEngine) GetEdge(graph, id string) *aql.Edge {
	return engine.Arachne.Graph(graph).GetEdge(id, true)
}

func (engine *GraphEngine) AddEdge(graph string, edge aql.Edge) error {
	return engine.Arachne.Graph(graph).SetEdge(edge)
}

func (engine *GraphEngine) AddVertex(graph string, vertex aql.Vertex) error {
	return engine.Arachne.Graph(graph).SetVertex(vertex)
}

/*
func (engine *GraphEngine) AddEdgeBundle(edgeBundle aql.EdgeBundle) error {
	return engine.DBI.SetEdgeBundle(edgeBundle)
}
*/

func (engine *GraphEngine) RunTraversal(ctx context.Context, query *aql.GraphQuery) (chan aql.ResultRow, error) {
	tr := engine.Query(query.Graph)
	//log.Printf("Starting Query: %#v", query.Query)
	for _, s := range query.Query {
		err := tr.RunStatement(s)
		if err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
	}
	return tr.GetResult(ctx)
}

func (engine *GraphEngine) Query(graph string) *Traversal {
	out := &Traversal{Query: engine.Arachne.Query(graph), ReadOnly: false}
	return out
}

type Traversal struct {
	ReadOnly bool
	Query    gdbi.QueryInterface
}

func (trav *Traversal) RunStatement(statement *aql.GraphStatement) error {
	/*
			if statement.GetAddV() != "" {
				trav.Query = trav.Query.AddV(statement.GetAddV())
			} else if statement.GetAddE() != "" {
				trav.Query = trav.Query.AddE(statement.GetAddE())
			} else if statement.GetTo() != "" {
				trav.Query = trav.Query.To(statement.GetTo())
			} else if x := statement.GetProperty(); x != nil {
					for k, v := range x.Fields {
						trav.Query = trav.Query.Property(k, v)
					}
			} else if _, ok := statement.GetStatement().(*aql.GraphStatement_Drop); ok {
				trav.Query = trav.Query.Drop()
		  } else
	*/
	if x, ok := statement.GetStatement().(*aql.GraphStatement_V); ok {
		if x.V == "" {
			trav.Query = trav.Query.V()
		} else {
			trav.Query = trav.Query.V(x.V)
		}
	} else if _, ok := statement.GetStatement().(*aql.GraphStatement_E); ok {
		trav.Query = trav.Query.E()
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Out); ok {
		if x.Out == "" {
			trav.Query = trav.Query.Out()
		} else {
			trav.Query = trav.Query.Out(x.Out)
		}
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_In); ok {
		if x.In == "" {
			trav.Query = trav.Query.In()
		} else {
			trav.Query = trav.Query.In(x.In)
		}
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_OutEdge); ok {
		if x.OutEdge == "" {
			trav.Query = trav.Query.OutE()
		} else {
			trav.Query = trav.Query.OutE(x.OutEdge)
		}
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_InEdge); ok {
		if x.InEdge == "" {
			trav.Query = trav.Query.InE()
		} else {
			trav.Query = trav.Query.InE(x.InEdge)
		}
	} else if x := statement.GetHas(); x != nil {
		trav.Query = trav.Query.Has(x.Key, x.Within...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Limit); ok {
		trav.Query = trav.Query.Limit(x.Limit)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Values); ok {
		trav.Query = trav.Query.Values(x.Values.Labels)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Import); ok {
		trav.Query = trav.Query.Import(x.Import)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Map); ok {
		trav.Query = trav.Query.Map(x.Map)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Fold); ok {
		trav.Query = trav.Query.Fold(x.Fold)
	} else if _, ok := statement.GetStatement().(*aql.GraphStatement_Count); ok {
		trav.Query = trav.Query.Count()
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_As); ok {
		trav.Query = trav.Query.As(x.As)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Select); ok {
		trav.Query = trav.Query.Select(x.Select.Labels)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_GroupCount); ok {
		trav.Query = trav.Query.GroupCount(x.GroupCount)
	} else {
		log.Printf("Unknown Statement: %#v", statement)
		return fmt.Errorf("Unknown Statement: %#v", statement)
	}
	return nil
}

func (trav *Traversal) GetResult(ctx context.Context) (chan aql.ResultRow, error) {
	e := trav.Query.Execute(ctx)
	if e == nil {
		return nil, fmt.Errorf("Query Failed")
	}
	return e, nil
}
