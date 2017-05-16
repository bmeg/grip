package graph

import (
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/ophion"
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
	v := ophion.Vertex{
		Gid:        id,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetVertex(v)
}

func (graph *Graph) AddEdge(out string, in string, prop map[string]interface{}) {
	e := ophion.Edge{
		Out:        out,
		In:         in,
		Properties: protoutil.AsStruct(prop),
	}
	graph.dbi.SetEdge(e)
}

func (graph *Graph) GetVertices() chan ophion.Vertex {
	return graph.dbi.GetVertexList()
}

func (graph *Graph) GetOutEdgesArray(id string) []ophion.Edge {
	out := []ophion.Edge{}
	for i := range graph.dbi.GetOutEdgeList(id, nil) {
		out = append(out, i)
	}
	return out
}

func (graph *Graph) GetInEdgesArray(id string) []ophion.Edge {
	out := []ophion.Edge{}
	for i := range graph.dbi.GetInEdgeList(id, nil) {
		out = append(out, i)
	}
	return out
}

func (graph *Graph) AggregateMessages(
	gen func(v ophion.Vertex, e ophion.Edge) interface{},
	agg func(v ophion.Vertex, msgs []interface{}) map[string]interface{},
) {

	collection := map[string][]interface{}{}
	for v := range graph.dbi.GetVertexList() {
		for e := range graph.dbi.GetOutEdgeList(v.Gid, nil) {
			i := gen(v, e)
			if _, ok := collection[e.In]; !ok {
				collection[e.In] = []interface{}{i}
			} else {
				collection[e.In] = append(collection[e.In], i)
			}
		}
		for e := range graph.dbi.GetInEdgeList(v.Gid, nil) {
			i := gen(v, e)
			if _, ok := collection[e.In]; !ok {
				collection[e.Out] = []interface{}{i}
			} else {
				collection[e.Out] = append(collection[e.Out], i)
			}
		}
	}
	for k, v := range collection {
		vert := graph.dbi.GetVertex(k)
		p := agg(*vert, v)
		protoutil.CopyToStruct(vert.Properties, p)
		graph.dbi.SetVertex(*vert)
	}
}
