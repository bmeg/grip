package gdbi

import (
	"github.com/bmeg/arachne/ophion"
)

type QueryInterface interface {
	V(key ...string) QueryInterface
	E() QueryInterface
	Count() QueryInterface
	Has(prop string, value ...string) QueryInterface
	Out(key ...string) QueryInterface
	In(key ...string) QueryInterface
	Limit(count int64) QueryInterface

	OutE(key ...string) QueryInterface
	InE(key ...string) QueryInterface

	As(label string) QueryInterface
	Select(labels []string) QueryInterface
	Values(labels []string) QueryInterface

	//code based functions
	Import(source string) QueryInterface
	Map(function string) QueryInterface
	Fold(function string) QueryInterface

	//Read write methods
	AddV(key string) QueryInterface
	AddE(key string) QueryInterface
	To(key string) QueryInterface
	Property(key string, value interface{}) QueryInterface
	Drop() QueryInterface

	Execute() chan ophion.ResultRow
	First() (ophion.ResultRow, error) //Only get one result
	Run() error                       //Do execute, but throw away the results
}

type ArachneInterface interface {
	Close()
	Query() QueryInterface
}

type DBI interface {
	GetVertex(key string) *ophion.Vertex
	GetVertexData(key string) *[]byte
	GetVertexList() chan ophion.Vertex
	GetEdgeList() chan ophion.Edge
	GetOutList(key string, filter EdgeFilter) chan ophion.Vertex
	GetInList(key string, filter EdgeFilter) chan ophion.Vertex

	GetOutEdgeList(key string, filter EdgeFilter) chan ophion.Edge
	GetInEdgeList(key string, filter EdgeFilter) chan ophion.Edge

	DelVertex(key string) error
	DelEdge(key string) error
	SetVertex(vertex ophion.Vertex) error
	SetEdge(edge ophion.Edge) error
}

type Traveler struct {
	State map[string]ophion.QueryResult
}

type EdgeFilter func(edge ophion.Edge) bool
type GraphPipe func() chan Traveler
