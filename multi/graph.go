package multi


import (
  "log"
  "fmt"
  "strings"
  "context"
  "path/filepath"
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
  outEdges  map[string][]*EdgeSource
  inEdges   map[string][]*EdgeSource
}

func NewTabularGraph(conf GraphConfig, idx *TableManager) (*TabularGraph,error) {
  out := TabularGraph{}

  out.idx = idx
  out.vertices = map[string]*VertexSource{}
  out.outEdges = map[string][]*EdgeSource{}
  out.inEdges  = map[string][]*EdgeSource{}

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

    tix, err := out.idx.NewDriver(t, table.Driver, fPath, *opt)
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
      if x, ok := out.outEdges[ e.FromVertex ]; ok {
        out.outEdges[e.FromVertex] = append(x, &es)
      } else {
        out.outEdges[e.FromVertex] = []*EdgeSource{&es}
      }
      if x, ok := out.inEdges[ e.ToVertex ]; ok {
        out.inEdges[e.ToVertex] = append(x, &es)
      } else {
        out.inEdges[e.ToVertex] = []*EdgeSource{&es}
      }
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

      if x, ok := out.outEdges[ e.ToVertex ]; ok {
        out.outEdges[e.ToVertex] = append(x, &es)
      } else {
        out.outEdges[e.ToVertex] = []*EdgeSource{&es}
      }
      if x, ok := out.inEdges[ e.FromVertex ]; ok {
        out.inEdges[e.FromVertex] = append(x, &es)
      } else {
        out.inEdges[e.FromVertex] = []*EdgeSource{&es}
      }

    }
  }
  return &out, nil
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
    for _, e := range i {
      out = append(out, e.label)
    }
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

func (t *TabularGraph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
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


func (t *TabularGraph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  out := make(chan gdbi.ElementLookup, 100)
  go func() {
    defer close(out)
    for r := range req {
      select {
      case <-ctx.Done():
      default:
        for vPrefix, edgeList := range t.outEdges {
          if strings.HasPrefix(r.ID, vPrefix) {
            for _, edge := range edgeList {
              if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.label) {
                log.Printf("Checkout edge %s %s", edge.fromVertex, edge.toVertex)
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
                log.Printf("GetOutChannel: %s %#v", edgeLabels, toVertex)

                if edge.toField == toVertex.config.PrimaryKey {
                  if row, err := edge.toDriver.GetRowByID(joinVal); err == nil {
                    outV := gripql.Vertex{Gid:toVertex.prefix + row.Key, Label:toVertex.label}
                    outV.Data = protoutil.AsStruct(row.Values)
                    r.Vertex = &outV
                    out <- r
                  }
                } else {
                  for row := range edge.toDriver.GetRowsByField(ctx, edge.toField, joinVal) {
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
      }
    }
  }()
  return out
}

func (t *TabularGraph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetInChannel")
  return nil
}

func (t *TabularGraph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetOutEdgeChannel")
  return nil
}


func (t *TabularGraph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
  log.Printf("Calling GetInEdgeChannel")
  return nil
}
