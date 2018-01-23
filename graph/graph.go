package graph

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/protoutil"
)

// Graph is a test api for easy graph operations using an in process instance
// of the arachne database
type Graph struct {
	dbi gdbi.DBI
}

// NewGraph creates a new Graph class given a DBI
func NewGraph(dbi gdbi.DBI) Graph {
	return Graph{dbi}
}

// NewMemGraph create a new Graph using the in memory DBI
func NewMemGraph() Graph {
	return NewGraph(memgraph.NewMemGDBI())
}

// AddVertex wraps dgbi.DBI.SetVertex
func (graph *Graph) AddVertex(id string, prop map[string]interface{}) {
	v := aql.Vertex{
		Gid:        id,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetVertex(v)
}

// UpdateVertex wraps dgbi.DBI.SetVertex
func (graph *Graph) UpdateVertex(v *aql.Vertex) {
	graph.dbi.SetVertex(*v)
}

// AddEdge wraps gdbi.DBI.SetEdge
func (graph *Graph) AddEdge(src string, dst string, prop map[string]interface{}) {
	e := aql.Edge{
		Src:        src,
		Dst:        dst,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetEdge(e)
}

// GetVertex gdbi.DBI.GetVertex
func (graph *Graph) GetVertex(id string) *aql.Vertex {
	return graph.dbi.GetVertex(id, true)
}

// GetVertices wraps gdbi.DBI.GetVertexList
func (graph *Graph) GetVertices() chan aql.Vertex {
	return graph.dbi.GetVertexList(context.Background(), true)
}

// GetOutEdgesArray wraps graph.dbi.GetOutEdgeList
func (graph *Graph) GetOutEdgesArray(id string) []aql.Edge {
	out := []aql.Edge{}
	for i := range graph.dbi.GetOutEdgeList(context.Background(), id, true, nil) {
		out = append(out, i)
	}
	return out
}

// GetInEdgesArray wraps graph.dbi.GetInEdgeList
func (graph *Graph) GetInEdgesArray(id string) []aql.Edge {
	out := []aql.Edge{}
	for i := range graph.dbi.GetInEdgeList(context.Background(), id, true, nil) {
		out = append(out, i)
	}
	return out
}

// AggregateMessages updates graph first by emitting messages and
// then reducing them on the receiver vertex
func (graph *Graph) AggregateMessages(
	gen func(v aql.Vertex, e aql.Edge) interface{},
	agg func(v aql.Vertex, msgs []interface{}) map[string]interface{},
) {

	collection := map[string][]interface{}{}
	for v := range graph.dbi.GetVertexList(context.Background(), true) {
		for e := range graph.dbi.GetOutEdgeList(context.Background(), v.Gid, true, nil) {
			i := gen(v, e)
			if _, ok := collection[e.Dst]; !ok {
				collection[e.Dst] = []interface{}{i}
			} else {
				collection[e.Dst] = append(collection[e.Dst], i)
			}
		}
		for e := range graph.dbi.GetInEdgeList(context.Background(), v.Gid, true, nil) {
			i := gen(v, e)
			if _, ok := collection[e.Dst]; !ok {
				collection[e.Src] = []interface{}{i}
			} else {
				collection[e.Src] = append(collection[e.Src], i)
			}
		}
	}
	for k, v := range collection {
		vert := graph.dbi.GetVertex(k, true)
		p := agg(*vert, v)
		protoutil.CopyToStruct(vert.Properties, p)
		graph.dbi.SetVertex(*vert)
	}
}
