package tabular

import (
  "log"
  "fmt"
  "context"
  "path/filepath"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/util/setcmp"
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
  out.vertices = map[string]*VertexSource{}
  out.outEdges = map[string]*EdgeSource{}
  out.inEdges  = map[string]*EdgeSource{}

  driverMap := map[string]Driver{}
  tableOptions := map[string]*Options{}
  log.Printf("Loading Table Conf")
  //Set up configs for different tables to be opened
  for name, table := range conf.Tables {
    tableOptions[ name ] = &Options{IndexedColumns: []string{}, Config:table.Config}
  }

  //add parameters to configs for the tables, based on how the vertices will use them
  for _, v := range conf.Vertices {
    if opt, ok := tableOptions[ v.Table ]; !ok {
      return nil, fmt.Errorf("Trying to use undeclared table: '%s'", v.Table)
    } else {
      if opt.PrimaryKey != "" && opt.PrimaryKey != v.PrimaryKey {
        //right now, only one vertex type can make a table (and declare its primary key type)
        return nil, fmt.Errorf("Table used by two vertex types: %s", v.Table)
      }
      opt.PrimaryKey = v.PrimaryKey
    }
  }

  //add parameters to configs for the tables, based on how the edges will use them
  for _, e := range conf.Edges {
    log.Printf("Edges: %s", e)
    toVertex := conf.Vertices[e.ToVertex]
    fromVertex := conf.Vertices[e.FromVertex]

    toTableOpts := tableOptions[toVertex.Table]
    fromTableOpts := tableOptions[fromVertex.Table]
    if toTableOpts == nil || fromTableOpts == nil {
      return nil, fmt.Errorf("Trying to use undeclared table")
    }
    if e.FromField != fromTableOpts.PrimaryKey {
      if !setcmp.ContainsString(fromTableOpts.IndexedColumns, e.FromField) {
        fromTableOpts.IndexedColumns = append(fromTableOpts.IndexedColumns, e.FromField)
      }
    }
    if e.ToField != toTableOpts.PrimaryKey {
      if !setcmp.ContainsString(toTableOpts.IndexedColumns, e.ToField) {
        toTableOpts.IndexedColumns = append(toTableOpts.IndexedColumns, e.ToField)
      }
    }
  }

  //open the table drivers
  for t, opt := range tableOptions {
    table := conf.Tables[ t ]

    log.Printf("Table: %s %#v", t, opt)
    fPath := filepath.Join( filepath.Dir(conf.path), table.Path )
    log.Printf("Loading: %s with primaryKey %s", fPath, opt.PrimaryKey)

    tix, err := out.idx.NewDriver(table.Driver, fPath, *opt)
    if err != nil {
      return nil, err
    }
    driverMap[t] = tix
  }

  //map the table drivers back onto the vertices that will use them
  for vPrefix, v := range conf.Vertices {
    log.Printf("Adding vertex prefix: %s label: %s", vPrefix, v.Label)
    tix := driverMap[v.Table]
    out.vertices[vPrefix] = &VertexSource{driver:tix, prefix:vPrefix, label:v.Label, config:&v}
  }

  for ePrefix, e := range conf.Edges {
    if e.Label != "" {
      toVertex := conf.Vertices[e.ToVertex]
      fromVertex := conf.Vertices[e.FromVertex]
      fromDriver := driverMap[fromVertex.Table]
      toDriver := driverMap[toVertex.Table]
      es := EdgeSource{
        label:e.Label,
        fromDriver:fromDriver,
        toDriver:toDriver, prefix:ePrefix,
        fromVertex:e.FromVertex, toVertex:e.ToVertex,
        fromField:e.FromField, toField:e.ToField }
      out.outEdges[ e.FromVertex ] = &es
      out.inEdges[ e.ToVertex ] = &es
    }
    if e.BackLabel != "" {
      toVertex := conf.Vertices[e.ToVertex]
      fromVertex := conf.Vertices[e.FromVertex]
      fromDriver := driverMap[fromVertex.Table]
      toDriver := driverMap[toVertex.Table]
      es := EdgeSource{
        label:e.BackLabel,
        fromDriver:toDriver,
        toDriver:fromDriver, prefix:ePrefix,
        fromVertex:e.ToVertex, toVertex:e.FromVertex,
        fromField:e.ToField, toField:e.FromField }
      out.outEdges[ e.ToVertex ] = &es
      out.inEdges[ e.FromVertex ] = &es
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
  return nil, fmt.Errorf("BuildSchema not implemented")
}

func (g *TabularGDB) Close() error {
    return g.graph.Close()
}
