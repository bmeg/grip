package memgraph

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
)

type edgepair struct {
	src string
	dst string
}

// MemGraph is a memory based graph driver
type MemGraph struct {
	vertices     map[string]*aql.Vertex
	outEdges     map[string]map[string][]string
	inEdges      map[string]map[string][]string
	edges        map[string]*aql.Edge
	edgeSequence int64
}

func NewMemGraph() *MemGraph {
	return &MemGraph{
		map[string]*aql.Vertex{},
		map[string]map[string][]string{},
		map[string]map[string][]string{},
		map[string]*aql.Edge{},
		0,
	}
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *MemGraph) GetVertex(key string, load bool) *aql.Vertex {
	return mg.vertices[key]
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *MemGraph) GetEdge(key string, load bool) *aql.Edge {
	return mg.edges[key]
}

// GetVertexList produces a channel of all edges in the graph
func (mg *MemGraph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	out := make(chan *aql.Vertex)
	go func() {
		defer close(out)
		for _, v := range mg.vertices {
			out <- v
		}
	}()
	return out
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *MemGraph) GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge {
	out := make(chan *aql.Edge)
	go func() {
		defer close(out)
		for _, src := range mg.outEdges {
			for _, dst := range src {
				for _, e := range dst {
					out <- mg.edges[e]
				}
			}
		}
	}()
	return out
}

// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetOutList(ctx context.Context, key string, load bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for dst, dstList := range mg.outEdges[key] {
			for range dstList {
					o <- *mg.vertices[dst]
			}
		}
	}()
	return o
}

// GetInList given vertex `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetInList(ctx context.Context, key string, load bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for src := range mg.inEdges[key] {
			for range mg.outEdges[src][key] {
					o <- *mg.vertices[src]
			}
		}
	}()
	return o
}

// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetOutEdgeList(ctx context.Context, key string, load bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for _, dstList := range mg.outEdges[key] {
			for _, dstEdge := range dstList {
					o <- *mg.edges[dstEdge]
			}
		}
	}()
	return o
}

// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetInEdgeList(ctx context.Context, key string, load bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for src := range mg.inEdges[key] {
			for _, srcEdge := range mg.outEdges[src][key] {
					o <- *mg.edges[srcEdge]
			}
		}
	}()
	return o
}

// DelVertex deletes vertex with id `key`
func (mg *MemGraph) DelVertex(key string) error {
	delete(mg.vertices, key)
	for k, elist := range mg.outEdges[key] {
		for _, e := range elist {
			delete(mg.edges, e)
		}
		delete(mg.inEdges[k], key)
	}
	delete(mg.outEdges, key)
	return nil
}

// DelEdge deletes edge with id `key`
func (mg *MemGraph) DelEdge(key string) error {
	p := mg.edges[key]
	for i := 0; i < len(mg.outEdges[p.From][p.To]); i++ {
		if mg.outEdges[p.From][p.To][i] == key {
			l := len(mg.outEdges[p.From][p.To])
			mg.outEdges[p.From][p.To][i] = mg.outEdges[p.From][p.To][l-1]
			mg.outEdges[p.From][p.To] = mg.outEdges[p.From][p.To][:l-1]
		}
	}
	for i := 0; i < len(mg.inEdges[p.To][p.From]); i++ {
		if mg.inEdges[p.From][p.To][i] == key {
			l := len(mg.inEdges[p.From][p.To])
			mg.inEdges[p.From][p.To][i] = mg.inEdges[p.From][p.To][l-1]
			mg.inEdges[p.From][p.To] = mg.inEdges[p.From][p.To][:l-1]
		}
	}
	delete(mg.edges, key)
	return nil
}

// SetVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *MemGraph) SetVertex(vertex *aql.Vertex) error {
	mg.vertices[vertex.Gid] = vertex
	return nil
}

// SetEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *MemGraph) SetEdge(edge *aql.Edge) error {
	if edge.Gid == "" {
		//BUG: this should check if the edge exists
		edge.Gid = fmt.Sprintf("%d", mg.edgeSequence)
		mg.edgeSequence++
	}
	mg.edges[edge.Gid] = edge

	if _, ok := mg.outEdges[edge.From]; !ok {
		mg.outEdges[edge.From] = map[string][]string{}
	}
	if _, ok := mg.outEdges[edge.From][edge.To]; ok {
		mg.outEdges[edge.From][edge.To] = append(mg.outEdges[edge.From][edge.To], edge.Gid)
	} else {
		mg.outEdges[edge.From][edge.To] = []string{edge.Gid}
	}

	if _, ok := mg.inEdges[edge.From]; !ok {
		mg.inEdges[edge.From] = map[string][]string{}
	}
	if _, ok := mg.inEdges[edge.From][edge.To]; ok {
		mg.inEdges[edge.From][edge.To] = append(mg.inEdges[edge.From][edge.To], edge.Gid)
	} else {
		mg.inEdges[edge.From][edge.To] = []string{edge.Gid}
	}
	return nil
}
