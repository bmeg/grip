
package memgraph;

import (
  "fmt"
	"github.com/bmeg/arachne/gdbi"
  "github.com/bmeg/arachne/ophion"
)

type edgepair struct {
  src string
  dst string
}

type MemGraph struct {
  vertices  map[string]*ophion.Vertex
  out_edges map[string]map[string][]*ophion.Edge
  in_edges  map[string]map[string][]string
  edges     map[string]edgepair
  edge_sequence int64
}

func NewMemGDBI() *MemGraph {
  return &MemGraph{
    map[string]*ophion.Vertex{},
    map[string]map[string][]*ophion.Edge{},
    map[string]map[string][]string{},
    map[string]edgepair{},
    0,
  }
}

func (self *MemGraph) GetVertex(key string) *ophion.Vertex {
  return self.vertices[key]
}

func (self *MemGraph) GetVertexList() chan ophion.Vertex {
  out := make(chan ophion.Vertex, 100)
  go func() {
    defer close(out)
    for _, v := range self.vertices {
      out <- *v
    }
  } ()
  return out
}

func (self *MemGraph) GetEdgeList() chan ophion.Edge {
  out := make(chan ophion.Edge, 100)
  go func() {
    defer close(out)
    for _, src := range self.out_edges {
      for _, dst := range src {
        for _, e := range dst {
          out <- *e
        }
      }
    }
  } ()
  return out
}

func (self *MemGraph) GetOutList(key string, filter gdbi.EdgeFilter) chan ophion.Vertex {
  	o := make(chan ophion.Vertex, 100)
  	go func() {
  		defer close(o)
      for dst, dst_list := range self.out_edges[key] {
        for _, dst_edge := range dst_list {
          send := false
          if filter != nil {
            if filter(*dst_edge) {
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
    } ()
  	return o
}


func (self *MemGraph) GetInList(key string, filter gdbi.EdgeFilter) chan ophion.Vertex {
  o := make(chan ophion.Vertex, 100)
  go func() {
    defer close(o)
    for src, _ := range self.in_edges[key] {
      for _, src_edge := range self.out_edges[src][key] {
        send := false
        if filter != nil {
          if filter(*src_edge) {
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
  } ()
  return o

}

func (self *MemGraph)	GetOutEdgeList(key string, filter gdbi.EdgeFilter) chan ophion.Edge {
  o := make(chan ophion.Edge, 100)
  go func() {
    defer close(o)
    for _, dst_list := range self.out_edges[key] {
      for _, dst_edge := range dst_list {
        send := false
        if filter != nil {
          if filter(*dst_edge) {
            send = true
          }
        } else {
          send = true
        }
        if send {
          o <- *dst_edge
        }
      }
    }
  } ()
  return o
}

func (self *MemGraph)	GetInEdgeList(key string, filter gdbi.EdgeFilter) chan ophion.Edge {
  o := make(chan ophion.Edge, 100)
  go func() {
    defer close(o)
    for src, _ := range self.in_edges[key] {
      for _, src_edge := range self.out_edges[src][key] {
        send := false
        if filter != nil {
          if filter(*src_edge) {
            send = true
          }
        } else {
          send = true
        }
        if send {
          o <- *src_edge
        }
      }
    }
  } ()
  return o
}

func (self *MemGraph)	DelVertex(key string) error {
  delete(self.vertices, key)
  for k, elist := range self.out_edges[key] {
    for _, e := range elist {
      delete(self.edges, e.Gid)
    }
    delete(self.in_edges[k], key)
  }
  delete(self.out_edges, key)
  return nil
}

func (self *MemGraph) DelEdge(key string) error {
  p := self.edges[key]
  for i := 0; i < len(self.out_edges[p.src][p.dst]); i++ {
    if self.out_edges[p.src][p.dst][i].Gid == key {
      l := len(self.out_edges[p.src][p.dst])
      self.out_edges[p.src][p.dst][i] = self.out_edges[p.src][p.dst][ l-1 ]
      self.out_edges[p.src][p.dst] = self.out_edges[p.src][p.dst][:l-1]
    }
  }
  for i := 0; i < len(self.in_edges[p.dst][p.src]); i++ {
    if self.in_edges[p.src][p.dst][i] == key {
      l := len(self.in_edges[p.src][p.dst])
      self.in_edges[p.src][p.dst][i] = self.in_edges[p.src][p.dst][ l-1 ]
      self.in_edges[p.src][p.dst] = self.in_edges[p.src][p.dst][:l-1]
    }
  }
  delete(self.edges, key)
  return nil
}

func (self *MemGraph) SetVertex(vertex ophion.Vertex) error {
  self.vertices[vertex.Gid] = &vertex
  return nil
}

func (self *MemGraph) SetEdge(edge ophion.Edge) error {
  edge.Gid = fmt.Sprintf("%d", self.edge_sequence)
  self.edge_sequence += 1
  self.edges[edge.Gid] = edgepair{src:edge.Out, dst:edge.In}

  if _, ok := self.out_edges[edge.Out]; !ok {
    self.out_edges[edge.Out] = map[string][]*ophion.Edge{}
  }
  if _, ok := self.out_edges[edge.Out][edge.In]; ok {
      self.out_edges[edge.Out][edge.In] = append( self.out_edges[edge.Out][edge.In], &edge )
  } else {
      self.out_edges[edge.Out][edge.In] = []*ophion.Edge{ &edge }
  }

  if _, ok := self.in_edges[edge.In]; !ok {
    self.in_edges[edge.In] = map[string][]string{}
  }
  if _, ok := self.in_edges[edge.In][edge.Out]; ok {
    self.in_edges[edge.In][edge.Out] = append( self.in_edges[edge.In][edge.Out], edge.Gid )
  } else {
    self.in_edges[edge.In][edge.Out] = []string{ edge.Gid }
  }
  return nil
}
