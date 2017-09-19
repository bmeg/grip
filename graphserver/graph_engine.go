package graphserver

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	"log"
)

type GraphEngine struct {
	Arachne gdbi.ArachneInterface
}

func NewGraphEngine(a gdbi.ArachneInterface) GraphEngine {
	return GraphEngine{Arachne: a}
}

func (engine *GraphEngine) AddGraph(graph string) error {
	return engine.Arachne.AddGraph(graph)
}

func (engine *GraphEngine) DeleteGraph(graph string) error {
	return engine.Arachne.DeleteGraph(graph)
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

func (engine *GraphEngine) GetBundle(graph, id string) *aql.Bundle {
	return engine.Arachne.Graph(graph).GetBundle(id, true)
}

func (engine *GraphEngine) AddEdge(graph string, edge aql.Edge) error {
	return engine.Arachne.Graph(graph).SetEdge(edge)
}

func (engine *GraphEngine) AddVertex(graph string, vertex aql.Vertex) error {
	return engine.Arachne.Graph(graph).SetVertex(vertex)
}

func (engine *GraphEngine) AddBundle(graph string, bundle aql.Bundle) error {
	return engine.Arachne.Graph(graph).SetBundle(bundle)
}

func (engine *GraphEngine) RunTraversal(ctx context.Context, query *aql.GraphQuery) (chan aql.ResultRow, error) {
	tr := engine.Query(query.Graph)
	for _, s := range query.Query {
		err := tr.RunStatement(s)
		if err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
	}
	return tr.GetResult(ctx)
}

func UnpackQuery(query *aql.GraphQuery, tr *Traversal) (*Traversal, error) {
	for _, s := range query.Query {
		err := tr.RunStatement(s)
		if err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
	}
	return tr, nil
}

func (engine *GraphEngine) Query(graph string) *Traversal {
	out := &Traversal{Query: engine.Arachne.Query(graph), engine: engine, graph: graph}
	return out
}

type Traversal struct {
	graph  string
	engine *GraphEngine
	Query  gdbi.QueryInterface
}

func (trav *Traversal) SubQuery() *Traversal {
	return &Traversal{Query: trav.engine.Arachne.Query(trav.graph), engine: trav.engine, graph: trav.graph}
}

func (trav *Traversal) RunStatement(statement *aql.GraphStatement) error {
	if x, ok := statement.GetStatement().(*aql.GraphStatement_V); ok {
		if x.V == "" {
			trav.Query = trav.Query.V()
		} else {
			trav.Query = trav.Query.V(x.V)
		}
	} else if _, ok := statement.GetStatement().(*aql.GraphStatement_E); ok {
		trav.Query = trav.Query.E()
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Out); ok {
		labels := protoutil.AsStringList(x.Out)
		trav.Query = trav.Query.Out(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_In); ok {
		labels := protoutil.AsStringList(x.In)
		trav.Query = trav.Query.In(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_OutEdge); ok {
		labels := protoutil.AsStringList(x.OutEdge)
		trav.Query = trav.Query.OutE(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_InEdge); ok {
		labels := protoutil.AsStringList(x.InEdge)
		trav.Query = trav.Query.InE(labels...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_OutBundle); ok {
		labels := protoutil.AsStringList(x.OutBundle)
		trav.Query = trav.Query.OutBundle(labels...)
	} else if x := statement.GetHas(); x != nil {
		trav.Query = trav.Query.Has(x.Key, x.Within...)
	} else if x, ok := statement.GetStatement().(*aql.GraphStatement_Labeled); ok {
		labels := protoutil.AsStringList(x.Labeled)
		trav.Query = trav.Query.Labeled(labels...)
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

func (trav *Traversal) GetResult(ctx context.Context) (chan aql.ResultRow, error) {
	e := trav.Query.Execute(ctx)
	if e == nil {
		return nil, fmt.Errorf("Query Failed")
	}
	return e, nil
}
