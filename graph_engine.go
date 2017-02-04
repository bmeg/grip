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
	//log.Printf("Starting Query")
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
	} else if statement.GetAddE() != "" {
		trav.Query = trav.Query.AddE(statement.GetAddE())
	} else if statement.GetTo() != "" {
		trav.Query = trav.Query.To(statement.GetTo())
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_V); ok {
		if x.V == "" {
			trav.Query = trav.Query.V()
		} else {
			trav.Query = trav.Query.V(x.V)
		}
	} else if _, ok := statement.GetStatement().(*ophion.GraphStatement_E); ok {
		trav.Query = trav.Query.E()
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_Out); ok {
		if x.Out == "" {
			trav.Query = trav.Query.Out()
		} else {
			trav.Query = trav.Query.Out(x.Out)
		}
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_In); ok {
		if x.In == "" {
			trav.Query = trav.Query.In()
		} else {
			trav.Query = trav.Query.In(x.In)
		}
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_OutEdge); ok {
		if x.OutEdge == "" {
			trav.Query = trav.Query.OutE()
		} else {
			trav.Query = trav.Query.OutE(x.OutEdge)
		}
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_InEdge); ok {
		if x.InEdge == "" {
			trav.Query = trav.Query.InE()
		} else {
			trav.Query = trav.Query.InE(x.InEdge)
		}
	} else if x := statement.GetHas(); x != nil {
		trav.Query = trav.Query.Has(x.Key, x.Within...)
	} else if x := statement.GetProperty(); x != nil {
		log.Printf("Property :%#v", x)
		for k, v := range x.Fields {
			log.Printf("Adding %s %s", k, v)
			trav.Query = trav.Query.Property(k, v)
		}
	} else if x, ok := statement.GetStatement().(*ophion.GraphStatement_Limit); ok {
		trav.Query = trav.Query.Limit(x.Limit)
	} else if _, ok := statement.GetStatement().(*ophion.GraphStatement_Count); ok {
		trav.Query = trav.Query.Count()
	} else if _, ok := statement.GetStatement().(*ophion.GraphStatement_Drop); ok {
		trav.Query = trav.Query.Drop()
	} else {
		log.Printf("Unknown Statement: %#v", statement)
	}

	return nil
}

func (trav *Traversal) GetResult() (chan ophion.QueryResult, error) {
	return trav.Query.Execute(), nil
}
