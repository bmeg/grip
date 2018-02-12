package graphserver

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"log"
)

// GraphEngine wraps the arachne interface and provides a traversal
// compiler that takes the traversal request data structure and changes
// it into a series of function calls
type GraphEngine struct {
	Arachne gdbi.ArachneInterface
}

// NewGraphEngine takes an ArachneInterface and returns a new graph engine
func NewGraphEngine(a gdbi.ArachneInterface) GraphEngine {
	return GraphEngine{Arachne: a}
}

// Close tell the arachne interface to close
func (engine *GraphEngine) Close() {
	engine.Arachne.Close()
}

// AddGraph wraps `ArachneInterface.AddGraph`
func (engine *GraphEngine) AddGraph(graph string) error {
	return engine.Arachne.AddGraph(graph)
}

// DeleteGraph wraps `ArachneInterface.DeleteGraph`
func (engine *GraphEngine) DeleteGraph(graph string) error {
	return engine.Arachne.DeleteGraph(graph)
}

// GetGraphs wraps `ArachneInterface.GetGraphs`
func (engine *GraphEngine) GetGraphs() []string {
	return engine.Arachne.GetGraphs()
}

// GetVertex wraps `ArachneInterface.GetVertex`
func (engine *GraphEngine) GetVertex(graph, id string) *aql.Vertex {
	return engine.Arachne.Graph(graph).GetVertex(id, true)
}

// GetEdge wraps `ArachneInterface.GetEdge`
func (engine *GraphEngine) GetEdge(graph, id string) *aql.Edge {
	return engine.Arachne.Graph(graph).GetEdge(id, true)
}

// GetBundle wraps `ArachneInterface.GetBundle`
func (engine *GraphEngine) GetBundle(graph, id string) *aql.Bundle {
	return engine.Arachne.Graph(graph).GetBundle(id, true)
}

// AddEdge wraps `ArachneInterface.AddEdge`
func (engine *GraphEngine) AddEdge(graph string, edge aql.Edge) error {
	return engine.Arachne.Graph(graph).SetEdge(edge)
}

// AddVertex wraps `ArachneInterface.AddVertex`
func (engine *GraphEngine) AddVertex(graph string, vertex aql.Vertex) error {
	return engine.Arachne.Graph(graph).SetVertex(vertex)
}

// AddBundle wraps `ArachneInterface.AddBundle`
func (engine *GraphEngine) AddBundle(graph string, bundle aql.Bundle) error {
	return engine.Arachne.Graph(graph).SetBundle(bundle)
}

// RunTraversal takes an aql.GraphQuery statement, compiles it and then executes it
func (engine *GraphEngine) RunTraversal(ctx context.Context, query *aql.GraphQuery) (chan aql.ResultRow, error) {

	SplitQuery(query)

	tr := engine.NewTraversal(query.Graph)
	for _, s := range query.Query {
		err := tr.AddStatement(s)
		if err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
	}
	return tr.GetResult(ctx)
}

// Query takes a graph name and initializes a new Traversal structure
func (engine *GraphEngine) NewTraversal(graph string) *Traversal {
	out := &Traversal{Query: engine.Arachne.Query(graph), engine: engine, graph: graph}
	return out
}


func SplitQuery(query *aql.GraphQuery) {
	if len(query.Query) == 0 {
		return
	}

	if len(query.Query) > 1 {
		first := query.Query[0]
		second := query.Query[1]
		if _, ok := first.GetStatement().(*aql.GraphStatement_V); ok {
			if _, ok := second.GetStatement().(*aql.GraphStatement_HasLabel); ok {
				log.Printf("Vertex Label Query")
			}
		}
	}

	first := query.Query[0]
	if _, ok := first.GetStatement().(*aql.GraphStatement_V); ok {
		log.Printf("Vertex Query")
	}


}
