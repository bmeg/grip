package memgraph

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
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

// NewMemGDBI creates new memory based ArachneInterface
func NewMemGDBI() *MemGraph {
	return &MemGraph{
		map[string]*aql.Vertex{},
		map[string]map[string][]string{},
		map[string]map[string][]string{},
		map[string]*aql.Edge{},
		0,
	}
}

// Query creates a QueryInterface for a particular Graph
func (mg *MemGraph) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(mg, false)
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
func (mg *MemGraph) GetVertexList(ctx context.Context, load bool) chan aql.Vertex {
	out := make(chan aql.Vertex, 100)
	go func() {
		defer close(out)
		for _, v := range mg.vertices {
			out <- *v
		}
	}()
	return out
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *MemGraph) GetEdgeList(ctx context.Context, load bool) chan aql.Edge {
	out := make(chan aql.Edge, 100)
	go func() {
		defer close(out)
		for _, src := range mg.outEdges {
			for _, dst := range src {
				for _, e := range dst {
					out <- *mg.edges[e]
				}
			}
		}
	}()
	return out
}

// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetOutList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for dst, dstList := range mg.outEdges[key] {
			for _, dstEdge := range dstList {
				send := false
				if filter != nil {
					if filter(*mg.edges[dstEdge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *mg.vertices[dst]
				}
			}
		}
	}()
	return o
}

// GetInList given vertex `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetInList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for src := range mg.inEdges[key] {
			for _, srcEdge := range mg.outEdges[src][key] {
				send := false
				if filter != nil {
					if filter(*mg.edges[srcEdge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *mg.vertices[src]
				}
			}
		}
	}()
	return o
}

// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetOutEdgeList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for _, dstList := range mg.outEdges[key] {
			for _, dstEdge := range dstList {
				send := false
				if filter != nil {
					if filter(*mg.edges[dstEdge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *mg.edges[dstEdge]
				}
			}
		}
	}()
	return o
}

// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *MemGraph) GetInEdgeList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for src := range mg.inEdges[key] {
			for _, srcEdge := range mg.outEdges[src][key] {
				send := false
				if filter != nil {
					if filter(*mg.edges[srcEdge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *mg.edges[srcEdge]
				}
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
	for i := 0; i < len(mg.outEdges[p.Src][p.Dst]); i++ {
		if mg.outEdges[p.Src][p.Dst][i] == key {
			l := len(mg.outEdges[p.Src][p.Dst])
			mg.outEdges[p.Src][p.Dst][i] = mg.outEdges[p.Src][p.Dst][l-1]
			mg.outEdges[p.Src][p.Dst] = mg.outEdges[p.Src][p.Dst][:l-1]
		}
	}
	for i := 0; i < len(mg.inEdges[p.Dst][p.Src]); i++ {
		if mg.inEdges[p.Src][p.Dst][i] == key {
			l := len(mg.inEdges[p.Src][p.Dst])
			mg.inEdges[p.Src][p.Dst][i] = mg.inEdges[p.Src][p.Dst][l-1]
			mg.inEdges[p.Src][p.Dst] = mg.inEdges[p.Src][p.Dst][:l-1]
		}
	}
	delete(mg.edges, key)
	return nil
}

// SetVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *MemGraph) SetVertex(vertex aql.Vertex) error {
	mg.vertices[vertex.Gid] = &vertex
	return nil
}

// SetEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *MemGraph) SetEdge(edge aql.Edge) error {
	if edge.Gid == "" {
		//BUG: this should check if the edge exists
		edge.Gid = fmt.Sprintf("%d", mg.edgeSequence)
		mg.edgeSequence++
	}
	mg.edges[edge.Gid] = &edge

	if _, ok := mg.outEdges[edge.Src]; !ok {
		mg.outEdges[edge.Src] = map[string][]string{}
	}
	if _, ok := mg.outEdges[edge.Src][edge.Dst]; ok {
		mg.outEdges[edge.Src][edge.Dst] = append(mg.outEdges[edge.Src][edge.Dst], edge.Gid)
	} else {
		mg.outEdges[edge.Src][edge.Dst] = []string{edge.Gid}
	}

	if _, ok := mg.inEdges[edge.Src]; !ok {
		mg.inEdges[edge.Src] = map[string][]string{}
	}
	if _, ok := mg.inEdges[edge.Src][edge.Dst]; ok {
		mg.inEdges[edge.Src][edge.Dst] = append(mg.inEdges[edge.Src][edge.Dst], edge.Gid)
	} else {
		mg.inEdges[edge.Src][edge.Dst] = []string{edge.Gid}
	}
	return nil
}
