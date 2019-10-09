package test

import (
  "os"
  "fmt"
  "strings"
  "testing"
  "context"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/grids"
  "github.com/bmeg/grip/engine"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/engine/inspect"
  "github.com/golang/protobuf/jsonpb"
)


var vertices = []string{
  `{"gid" : "1", "label" : "Person", "data" : { "name" : "bob" }}`,
  `{"gid" : "2", "label" : "Person", "data" : { "name" : "alice" }}`,
  `{"gid" : "3", "label" : "Person", "data" : { "name" : "jane" }}`,
  `{"gid" : "4", "label" : "Person", "data" : { "name" : "janet" }}`,
}

var edges = []string{
  `{"gid" : "e1", "label" : "knows", "from" : "1", "to" : "2", "data" : {}}`,
  `{"gid" : "e3", "label" : "knows", "from" : "2", "to" : "3", "data" : {}}`,
  `{"gid" : "e4", "label" : "knows", "from" : "3", "to" : "4", "data" : {}}`,
}

func SelectPath(stmts []*gripql.GraphStatement, steps []string, path []string) []*gripql.GraphStatement {
  out := []*gripql.GraphStatement{}
  for _, p := range path {
    for i := range steps {
      if steps[i] == p {
        out = append(out, stmts[i])
      }
    }
  }
  return out
}


func TestPath2Step(t *testing.T) {
  q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))

  ps := gdbi.NewPipelineState(q.Statements)

  steps := inspect.PipelineSteps(q.Statements)
  noLoadPaths := inspect.PipelineNoLoadPathSteps(q.Statements, 2)

  if len(noLoadPaths) > 0 {
  	fmt.Printf("Found Path: %s\n", noLoadPaths)
    path := SelectPath(q.Statements, steps, noLoadPaths[0])
    proc := grids.RawPathCompile( nil, ps, path )
    fmt.Printf("Proc: %s\n", proc)
  }
}

func TestEngineQuery(t *testing.T) {
  gdb, err := grids.NewGridsGraphDB("testing.db")
  if err != nil {
    t.Error(err)
  }

  gdb.AddGraph("test")
  graph, err := gdb.Graph("test")
  if err != nil {
    t.Error(err)
  }

  m := jsonpb.Unmarshaler{}

  vset := []*gripql.Vertex{}
  for _, r := range vertices {
    v := &gripql.Vertex{}
    err := m.Unmarshal(strings.NewReader(r), v)
    if err != nil {
      t.Error(err)
    }
    vset  = append(vset, v)
  }
  graph.AddVertex(vset)

  eset := []*gripql.Edge{}
  for _, r := range edges {
    e := &gripql.Edge{}
    err := m.Unmarshal(strings.NewReader(r), e)
    if err != nil {
      t.Error(err)
    }
    eset = append(eset, e)
  }
  graph.AddEdge(eset)


  q := gripql.NewQuery()
	q = q.V().Out().Out().Count()
  comp := graph.Compiler()

  pipeline, err := comp.Compile(q.Statements)
  if err != nil {
    t.Error(err)
  }

  out := engine.Run(context.Background(), pipeline, "./work.dir")

  for r := range out {
    fmt.Printf("result: %s\n", r)
  }

  gdb.Close()
  os.RemoveAll("testing.db")
}
