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

func NewGDB(conf *GraphConfig, indexPath string) *TabularGDB {
  out := TabularGraph{}
  out.idx, _ = NewTablularIndex(indexPath)
  out.vertices = map[string]*Table{}
  out.edges = []*EdgeConfig{}

  for _, t := range conf.Tables {
    log.Printf("Table: %s", t)
    fPath := filepath.Join( filepath.Dir(conf.path), t.Path )
    log.Printf("Loading: %s with primaryKey %s", fPath, t.PrimaryKey)
    tix := out.idx.IndexTSV(fPath, t.PrimaryKey, []string{})
    if t.Label != "" {
      out.vertices[t.Name] = &Table{data:tix, prefix:t.Prefix, label:t.Label, inEdges:[]*EdgeConfig{}, outEdges:[]*EdgeConfig{}, config:&t}
    }
  }

  for _, t := range conf.Tables {
    for _, e := range t.Edges {
      out.edges = append(out.edges, &e)
      if e.ToTable != "" {
        tt := out.vertices[e.ToTable]
        out.vertices[e.ToTable].inEdges = append( out.vertices[e.ToTable].inEdges, &EdgeConfig{ ToTable:e.ToTable, Label:e.Label, From:tt.config.PrimaryKey })
        out.vertices[t.Name].outEdges = append( out.vertices[t.Name].outEdges, &EdgeConfig{ ToTable:e.ToTable, Label:e.Label, To:e.To, From:t.PrimaryKey })
      }
      if e.FromTable != "" {
        ft := out.vertices[e.FromTable]
        out.vertices[e.FromTable].outEdges = append( out.vertices[e.FromTable].outEdges, &EdgeConfig{ ToTable:e.ToTable, Label:e.Label, From:ft.config.PrimaryKey })
        out.vertices[t.Name].inEdges = append( out.vertices[t.Name].inEdges, &EdgeConfig{ ToTable:e.ToTable, Label:e.Label, From:t.PrimaryKey })
      }
    }
  }

  return &TabularGDB{&out}
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
