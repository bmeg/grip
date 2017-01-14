package arachne

import (
  "log"
  "github.com/bmeg/arachne/gdbi"
  "github.com/bmeg/arachne/ophion"
  "golang.org/x/net/context"
)

type GraphEngine struct {
  DBI    gdbi.ArachneInterface
}

func NewGraphEngine(dbi gdbi.ArachneInterface) GraphEngine {
  return GraphEngine{DBI:dbi}
}


func (engine *GraphEngine) RunTraversal(ctx context.Context, query *ophion.GraphQuery) (*ophion.QueryResult, error) {
  log.Printf("Starting Query")
  tr := Traversal{DBI:engine.DBI, ReadOnly:false}
  tr.StartQuery()
  for _, s := range query.Query {
    tr.RunStatement(s)
  }
  return tr.GetResult()
}

func (engine *GraphEngine) Query() *Traversal {
  out := &Traversal{DBI:engine.DBI, ReadOnly:false}
  out.StartQuery()
  return out
}

type Traversal struct {
  ReadOnly bool
  DBI gdbi.ArachneInterface  
  Query gdbi.QueryInterface
}

func (trav *Traversal) RunStatement(statement *ophion.GraphStatement) error {
  if (trav.ReadOnly) {
    if statement.GetAddV() != "" {
      trav.Query = trav.Query.AddV(statement.GetAddV())
    }
  }
  return nil
}

func (trav *Traversal) StartQuery()  (error) {
  var err error
  
  return err
}

func (trav *Traversal) GetResult()  (*ophion.QueryResult, error) {
  return &ophion.QueryResult{}, nil
}
