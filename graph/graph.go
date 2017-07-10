package graph

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/protoutil"
)

type Graph struct {
	dbi gdbi.DBI
}

func NewGraph(dbi gdbi.DBI) Graph {
	return Graph{dbi}
}

func NewMemGraph() Graph {
	return NewGraph(memgraph.NewMemGDBI())
}

func (graph *Graph) AddVertex(id string, prop map[string]interface{}) {
	v := aql.Vertex{
		Gid:        id,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetVertex(v)
}

func (graph *Graph) UpdateVertex(v *aql.Vertex) {
	graph.dbi.SetVertex(*v)
}

func (graph *Graph) AddEdge(src string, dst string, prop map[string]interface{}) {
	e := aql.Edge{
		Src:        src,
		Dst:        dst,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetEdge(e)
}

func (graph *Graph) GetVertex(id string) *aql.Vertex {
	return graph.dbi.GetVertex(id, true)
}

func (graph *Graph) GetVertices() chan aql.Vertex {
	return graph.dbi.GetVertexList(context.Background(), true)
}

func (graph *Graph) GetOutEdgesArray(id string) []aql.Edge {
	out := []aql.Edge{}
	for i := range graph.dbi.GetOutEdgeList(context.Background(), id, true, nil) {
		out = append(out, i)
	}
	return out
}

func (graph *Graph) GetInEdgesArray(id string) []aql.Edge {
	out := []aql.Edge{}
	for i := range graph.dbi.GetInEdgeList(context.Background(), id, true, nil) {
		out = append(out, i)
	}
	return out
}

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
