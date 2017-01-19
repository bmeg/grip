package arachne

import (
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/ophion"
	//"golang.org/x/net/context"
	"log"
)

type GraphEngine struct {
	DBI gdbi.ArachneInterface
}

func NewGraphEngine(dbi gdbi.ArachneInterface) GraphEngine {
	return GraphEngine{DBI: dbi}
}

func (engine *GraphEngine) RunTraversal(query *ophion.GraphQuery) (chan ophion.QueryResult, error) {
	log.Printf("Starting Query")
	tr := engine.Query()
	for _, s := range query.Query {
		tr.RunStatement(s)
	}
	return tr.GetResult()
}

func (engine *GraphEngine) Query() *Traversal {
	out := &Traversal{DBI: engine.DBI, ReadOnly: false, Query: engine.DBI.Query()}
	return out
}

type Traversal struct {
	ReadOnly bool
	DBI      gdbi.ArachneInterface
	Query    gdbi.QueryInterface
}

func (trav *Traversal) RunStatement(statement *ophion.GraphStatement) error {
	if statement.GetAddV() != "" {
		trav.Query = trav.Query.AddV(statement.GetAddV())
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_V); ok {
		if x.V == "" {
			trav.Query = trav.Query.V()
		} else {
			trav.Query = trav.Query.V(x.V)
		}
	} else if _, ok := statement.GetStatement().(*ophion.GraphStatement_Count); ok {
		trav.Query = trav.Query.Count()
	} else {
		log.Printf("Unknown Statement: %#v", statement)
	}

	return nil
}

func (trav *Traversal) GetResult() (chan ophion.QueryResult, error) {
	return trav.Query.Execute(), nil
}
