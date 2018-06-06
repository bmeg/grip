package sql

import (
	"context"
	"errors"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
)

// Graph is the interface to a single graph
type Graph struct {
	db     *SQL
	ts     *timestamp.Timestamp
	graph  string
	schema *Schema
}

// Compiler returns a query compiler that uses the graph
func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g)
}

// GetTimestamp gets the timestamp of last update
func (g *Graph) GetTimestamp() string {
	return g.ts.Get(g.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (g *Graph) GetVertex(key string, load bool) *aql.Vertex {
	// keys will be of the form: <label>:<primary_key>
	return nil
}

// GetEdge loads an edge given an id. It returns nil if not found
func (g *Graph) GetEdge(key string, load bool) *aql.Edge {
	// keys will be of the form: <label>:<primary_key>
	return nil
}

// GetVertexList produces a channel of all vertices in the graph
func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	o := make(chan string, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (g *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (g *Graph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (g *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (g *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (g *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (g *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

////////////////////////////////////////////////////////////////////////////////
// Write methods are not implemented
////////////////////////////////////////////////////////////////////////////////

// AddVertex is not implemented in the SQL driver
func (g *Graph) AddVertex(vertexArray []*aql.Vertex) error {
	return errors.New("not implemented")
}

// AddEdge is not implemented in the SQL driver
func (g *Graph) AddEdge(edgeArray []*aql.Edge) error {
	return errors.New("not implemented")
}

// DelVertex is not implemented in the SQL driver
func (g *Graph) DelVertex(key string) error {
	return errors.New("not implemented")
}

// DelEdge is not implemented in the SQL driver
func (g *Graph) DelEdge(key string) error {
	return errors.New("not implemented")
}
