
package graph;


import (
  "github.com/bmeg/arachne/gdbi"
  "github.com/bmeg/arachne/ophion"
  "github.com/bmeg/arachne/protoutil"
  "github.com/bmeg/arachne/memgraph"
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
    Gid:id,
    Properties: protoutil.AsStruct(prop),
  }
  graph.dbi.SetVertex(v) 
}

func (graph *Graph) AddEdge(out string, in string, prop map[string]interface{}) {
  e := ophion.Edge{
    Out: out,
    In: in, 
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
  func(v ophion.Vertex, e ophion.Edge) interface{},
  func(v ophion.Vertex, msgs []interface{}) map[string]interface{},
) *Graph {
  //TODO: Actually implement this
  return nil
}


