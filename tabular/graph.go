package tabular


import (
  "log"
  "fmt"
  "strings"
  "context"
  "github.com/bmeg/grip/util/setcmp"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/protoutil"
  "github.com/bmeg/grip/engine/core"
)


type VertexSource struct {
  driver Driver
  prefix string
  label  string
  config   *VertexConfig
}

type EdgeSource struct {
  fromDriver   Driver
  toDriver     Driver
  fromVertex   string
  toVertex     string
  prefix       string
  label        string
  fromField    string
  toField      string
}

type TabularGraph struct {
  idx       *TableManager
  vertices  map[string]*VertexSource
  outEdges  map[string]*EdgeSource
  inEdges   map[string]*EdgeSource
}


func (t *TabularGraph) Close() error {
  return nil
}

func (t *TabularGraph) AddVertex(vertex []*gripql.Vertex) error {
  return fmt.Errorf("AddVertex not implemented")
}


func (t *TabularGraph) AddEdge(edge []*gripql.Edge) error {
  return fmt.Errorf("AddEdge not implemented")
}


func (t *TabularGraph) BulkAdd(stream <-chan *gripql.GraphElement) error {
  return fmt.Errorf("BulkAdd not implemented")
}

func (t *TabularGraph) Compiler() gdbi.Compiler {
  return core.NewCompiler(t, TabularOptimizer)
}

func (t *TabularGraph) GetTimestamp() string {
  return "NA"
}

func (t *TabularGraph) GetVertex(key string, load bool) *gripql.Vertex {
  for _, v := range t.vertices {
    if strings.HasPrefix(key, v.prefix) {
      id := key[len(v.prefix):len(key)]
      driver := v.driver
      if row, err := driver.GetRowByID(id); err == nil {
        o := gripql.Vertex{ Gid: v.prefix + row.Key, Label: v.label, Data:protoutil.AsStruct(row.Values) }
        return &o
      } else {
        log.Printf("Row not read")
      }
    }
  }
  return nil
}

func (t *TabularGraph) GetEdge(key string, load bool) *gripql.Edge {
  return nil
}

func (t *TabularGraph) DelVertex(key string) error {
  return fmt.Errorf("DelVertex not implemented")

}

func (t *TabularGraph) DelEdge(key string) error {
  return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) VertexLabelScan(ctx context.Context, label string) chan string {
  out := make(chan string, 10)
  go func() {
    defer close(out)
    for _, t := range t.vertices {
      if t.label == label {
        for n := range t.driver.GetIDs(ctx) {
          out <- t.prefix + n
        }
      }
    }
  }()
  return out
}

func (t *TabularGraph) ListVertexLabels() ([]string, error) {
  out := []string{}
  for _, i := range t.vertices {
    out = append(out, i.label)
  }
  return out, nil
}

func (t *TabularGraph) ListEdgeLabels() ([]string, error) {
  out := []string{}
  for _, i := range t.outEdges {
    out = append(out, i.label)
  }
  return out, nil
}

func (t *TabularGraph) AddVertexIndex(label string, field string) error {
  return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) DeleteVertexIndex(label string, field string) error {
  return fmt.Errorf("DelEdge not implemented")
}


func (t *TabularGraph) GetVertexIndexList() <-chan *gripql.IndexID {
  log.Printf("Calling GetVertexIndexList")
  return nil
}


func (t *TabularGraph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
  out := make(chan *gripql.Vertex, 100)
  go func() {
    for _, table := range t.vertices {
      log.Printf("table: %s", table.label)
      for row := range table.driver.GetRows(ctx) {
        v := gripql.Vertex{ Gid: table.prefix + row.Key, Label: table.label, Data:protoutil.AsStruct(row.Values) }
        out <- &v
      }
    }
    defer close(out)
  }()
  return out
}


func (t *TabularGraph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
  log.Printf("Calling GetEdgeList")
  return nil
}

func (t *TabularGraph) GetVertexChannel(req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
  out := make(chan gdbi.ElementLookup, 10)
  go func() {
    defer close(out)
    for r := range req {
      for _, v := range t.vertices {
        if strings.HasPrefix(r.ID, v.prefix) {
          id := r.ID[len(v.prefix):len(r.ID)]
          o := gripql.Vertex{Gid:r.ID, Label:v.label}
          if row, err:= v.driver.GetRowByID(id); err == nil {
            o.Data = protoutil.AsStruct(row.Values)
            r.Vertex = &o
            out <- r
          }
        }
      }
    }
  }()
  return out
}


func (t *TabularGraph) GetOutChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  out := make(chan gdbi.ElementLookup, 10)
  go func() {
    defer close(out)
    for r := range req {
      for vPrefix, edge := range t.outEdges {
        if strings.HasPrefix(r.ID, vPrefix) {
          if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.label) {
            id := r.ID[len(vPrefix):len(r.ID)]

            fromVertex := t.vertices[ edge.fromVertex ]

            joinVal := ""
            if edge.fromField == fromVertex.config.PrimaryKey {
              joinVal = id
            } else {
              elem := r.Ref.GetCurrent()
              joinVal = elem.Data[edge.fromField].(string)
            }
            toVertex := t.vertices[ edge.toVertex ]
            if edge.toField == toVertex.config.PrimaryKey {
              if row, err := edge.toDriver.GetRowByID(joinVal); err == nil {
                outV := gripql.Vertex{Gid:toVertex.prefix + row.Key, Label:toVertex.label}
                outV.Data = protoutil.AsStruct(row.Values)
                r.Vertex = &outV
                out <- r
              }
            } else {
              for row := range edge.toDriver.GetRowsByField(context.TODO(), edge.toField, joinVal) {
                outV := gripql.Vertex{Gid:toVertex.prefix + row.Key, Label:toVertex.label}
                outV.Data = protoutil.AsStruct(row.Values)
                r.Vertex = &outV
                out <- r
              }
            }
          }

        }
      }
    }
  }()
  return out
}

func (t *TabularGraph) GetInChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetInChannel")
  return nil
}

func (t *TabularGraph) GetOutEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetOutEdgeChannel")
  return nil
}


func (t *TabularGraph) GetInEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetInEdgeChannel")
  return nil
}
