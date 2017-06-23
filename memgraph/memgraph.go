package memgraph

import (
	"fmt"
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
)

type edgepair struct {
	src string
	dst string
}

type MemGraph struct {
	vertices      map[string]*aql.Vertex
	out_edges     map[string]map[string][]string
	in_edges      map[string]map[string][]string
	edges         map[string]*aql.Edge
	edge_sequence int64
}

func NewMemGDBI() *MemGraph {
	return &MemGraph{
		map[string]*aql.Vertex{},
		map[string]map[string][]string{},
		map[string]map[string][]string{},
		map[string]*aql.Edge{},
		0,
	}
}

func (self *MemGraph) GetVertex(key string) *aql.Vertex {
	return self.vertices[key]
}

func (self *MemGraph) GetVertexList(ctx context.Context, load bool) chan aql.Vertex {
	out := make(chan aql.Vertex, 100)
	go func() {
		defer close(out)
		for _, v := range self.vertices {
			out <- *v
		}
	}()
	return out
}

func (self *MemGraph) GetEdgeList() chan aql.Edge {
	out := make(chan aql.Edge, 100)
	go func() {
		defer close(out)
		for _, src := range self.out_edges {
			for _, dst := range src {
				for _, e := range dst {
					out <- *self.edges[e]
				}
			}
		}
	}()
	return out
}

func (self *MemGraph) GetOutList(key string, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for dst, dst_list := range self.out_edges[key] {
			for _, dst_edge := range dst_list {
				send := false
				if filter != nil {
					if filter(*self.edges[dst_edge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *self.vertices[dst]
				}
			}
		}
	}()
	return o
}

func (self *MemGraph) GetInList(key string, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		for src, _ := range self.in_edges[key] {
			for _, src_edge := range self.out_edges[src][key] {
				send := false
				if filter != nil {
					if filter(*self.edges[src_edge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *self.vertices[src]
				}
			}
		}
	}()
	return o

}

func (self *MemGraph) GetOutEdgeList(key string, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for _, dst_list := range self.out_edges[key] {
			for _, dst_edge := range dst_list {
				send := false
				if filter != nil {
					if filter(*self.edges[dst_edge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *self.edges[dst_edge]
				}
			}
		}
	}()
	return o
}

func (self *MemGraph) GetInEdgeList(key string, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		for src, _ := range self.in_edges[key] {
			for _, src_edge := range self.out_edges[src][key] {
				send := false
				if filter != nil {
					if filter(*self.edges[src_edge]) {
						send = true
					}
				} else {
					send = true
				}
				if send {
					o <- *self.edges[src_edge]
				}
			}
		}
	}()
	return o
}

func (self *MemGraph) DelVertex(key string) error {
	delete(self.vertices, key)
	for k, elist := range self.out_edges[key] {
		for _, e := range elist {
			delete(self.edges, e)
		}
		delete(self.in_edges[k], key)
	}
	delete(self.out_edges, key)
	return nil
}

func (self *MemGraph) DelEdge(key string) error {
	p := self.edges[key]
	for i := 0; i < len(self.out_edges[p.Src][p.Dst]); i++ {
		if self.out_edges[p.Src][p.Dst][i].Gid == key {
			l := len(self.out_edges[p.Src][p.Dst])
			self.out_edges[p.Src][p.Dst][i] = self.out_edges[p.Src][p.Dst][l-1]
			self.out_edges[p.Src][p.Dst] = self.out_edges[p.Src][p.Dst][:l-1]
		}
	}
	for i := 0; i < len(self.in_edges[p.Dst][p.Src]); i++ {
		if self.in_edges[p.Src][p.Dst][i] == key {
			l := len(self.in_edges[p.Src][p.Dst])
			self.in_edges[p.Src][p.Dst][i] = self.in_edges[p.Src][p.Dst][l-1]
			self.in_edges[p.Src][p.Dst] = self.in_edges[p.Src][p.Dst][:l-1]
		}
	}
	delete(self.edges, key)
	return nil
}

func (self *MemGraph) SetVertex(vertex aql.Vertex) error {
	self.vertices[vertex.Gid] = &vertex
	return nil
}

func (self *MemGraph) SetEdge(edge aql.Edge) error {
	edge.Gid = fmt.Sprintf("%d", self.edge_sequence)
	self.edge_sequence += 1
	self.edges[edge.Gid] = edgepair{src: edge.Src, dst: edge.Dst}

	if _, ok := self.out_edges[edge.Src]; !ok {
		self.out_edges[edge.Src] = map[string][]*aql.Edge{}
	}
	if _, ok := self.out_edges[edge.Src][edge.Dst]; ok {
		self.out_edges[edge.Src][edge.Dst] = append(self.out_edges[edge.Src][edge.Dst], &edge)
	} else {
		self.out_edges[edge.Src][edge.Dst] = []*aql.Edge{&edge}
	}

	if _, ok := self.in_edges[edge.Src]; !ok {
		self.in_edges[edge.Src] = map[string][]string{}
	}
	if _, ok := self.in_edges[edge.Src][edge.Dst]; ok {
		self.in_edges[edge.Src][edge.Dst] = append(self.in_edges[edge.Src][edge.Dst], edge.Gid)
	} else {
		self.in_edges[edge.Src][edge.Dst] = []string{edge.Gid}
	}
	return nil
}
