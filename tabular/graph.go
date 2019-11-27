package tabular


import (
  "log"
  "fmt"
  "context"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/protoutil"
  "github.com/bmeg/grip/engine/core"
)


type Table struct {
  data *TSVIndex
  prefix string
  label  string
}

type TabularGraph struct {
  idx *TabularIndex
  tables map[string]*Table
}


func (t *TabularGraph) Close() error {
  t.idx.Close()
  return nil
}

func (t *TabularGraph) AddVertex(vertex []*gripql.Vertex) error {
  return fmt.Errorf("AddVertex not implemented")
}


func (t *TabularGraph) AddEdge(edge []*gripql.Edge) error {
  return fmt.Errorf("AddEdge not implemented")
}


func (t *TabularGraph) Compiler() gdbi.Compiler {
  return core.NewCompiler(t)
}

func (t *TabularGraph) GetTimestamp() string {
  return "NA"
}

func (t *TabularGraph) GetVertex(key string, load bool) *gripql.Vertex {
  log.Printf("Calling GetVertex")
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
    for _, t := range t.tables {
      if t.label == label {

      }
    }
  }()

  return nil
}

func (t *TabularGraph) ListVertexLabels() ([]string, error) {
  out := []string{}
  for _, i := range t.tables {
    out = append(out, i.label)
  }
  return out, nil
}

func (t *TabularGraph) ListEdgeLabels() ([]string, error) {
  return []string{}, nil
}

func (t *TabularGraph) AddVertexIndex(label string, field string) error {
  return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) DeleteVertexIndex(label string, field string) error {
  return fmt.Errorf("DelEdge not implemented")
}


func (t *TabularGraph) GetVertexIndexList() <-chan *gripql.IndexID {
  return nil
}


func (t *TabularGraph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
  out := make(chan *gripql.Vertex, 100)
  go func() {
    log.Printf("Listing Vertices")
    for _, table := range t.tables {
      for row := range table.data.GetRows() {
        //log.Printf("V: %s", row.Key)
        v := gripql.Vertex{ Gid: table.prefix + row.Key, Label: table.label, Data:protoutil.AsStringStruct(row.Values) }
        out <- &v
      }
    }
    defer close(out)
  }()
  return out
}


func (t *TabularGraph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
  return nil
}

func (t *TabularGraph) GetVertexChannel(req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
  log.Printf("Calling GetVertexChannel")
  return nil
}


func (t *TabularGraph) GetOutChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  return nil
}

func (t *TabularGraph) GetInChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  return nil
}

func (t *TabularGraph) GetOutEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  return nil
}


func (t *TabularGraph) GetInEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  return nil
}
