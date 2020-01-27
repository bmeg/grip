package tabular

import (
  "log"
  "fmt"
  "context"
  "path/filepath"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/gripql"
)

type TabularGDB struct {
  graph *TabularGraph
}

func NewGDB(conf *GraphConfig, indexPath string) (*TabularGDB, error) {
  out := TabularGraph{}
  idx, err := NewTableManager(indexPath)
  if err != nil {
    return nil, err
  }
  out.idx = idx
  out.vertices = map[string]*Table{}
  out.edges = []*EdgeConfig{}

  for _, t := range conf.Tables {
    log.Printf("Table: %s", t)
    fPath := filepath.Join( filepath.Dir(conf.path), t.Path )
    log.Printf("Loading: %s with primaryKey %s", fPath)
    tix, err := out.idx.NewDriver(t.Driver, fPath, Options{PrimaryKey: t.PrimaryKey, IndexedColumns: []string{} })
    if err != nil {
      return nil, err
    }
    if t.Label != "" {
      out.vertices[t.Name] = &Table{driver:tix, prefix:t.Prefix, label:t.Label, inEdges:[]*EdgeConfig{}, outEdges:[]*EdgeConfig{}, config:&t}
    }
  }

  for _, t := range conf.Tables {
    for _, e := range t.Edges {
      out.edges = append(out.edges, &e)
      if e.ToTable != "" {
        tt := out.vertices[e.ToTable]
        out.vertices[e.ToTable].inEdges = append( out.vertices[e.ToTable].inEdges, &EdgeConfig{ FromTable:e.ToTable, Label:e.Label, From:tt.config.PrimaryKey })
        out.vertices[t.Name].outEdges = append( out.vertices[t.Name].outEdges, &EdgeConfig{ ToTable:t.Name, Label:e.Label, To:e.To, From:t.PrimaryKey })
      }
      if e.FromTable != "" {
        ft := out.vertices[e.FromTable]
        out.vertices[e.FromTable].outEdges = append( out.vertices[e.FromTable].outEdges, &EdgeConfig{ ToTable:ft.config.Name, Label:e.Label, To:ft.config.PrimaryKey })
        out.vertices[t.Name].inEdges = append( out.vertices[t.Name].inEdges, &EdgeConfig{ FromTable:e.FromTable, Label:e.Label, From:ft.config.PrimaryKey })
      }
    }
  }

  return &TabularGDB{&out}, nil
}


func (g *TabularGDB) AddGraph(string) error {
  return fmt.Errorf("AddGraph not implemented")
}

func (g *TabularGDB) DeleteGraph(string) error {
  return fmt.Errorf("AddGraph not implemented")
}


func (g *TabularGDB) ListGraphs() []string {
  return []string{"main"}
}

func (g *TabularGDB) Graph(graphID string) (gdbi.GraphInterface, error) {
  return g.graph, nil
}

func (g *TabularGDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
  return nil, fmt.Errorf("AddGraph not implemented")
}

func (g *TabularGDB) Close() error {
    return g.graph.Close()
}
