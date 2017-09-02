package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
)

type QueryInterface interface {
	V(key ...string) QueryInterface
	E() QueryInterface
	Count() QueryInterface
	Labeled(labels ...string) QueryInterface
	Has(prop string, value ...string) QueryInterface
	Out(key ...string) QueryInterface
	In(key ...string) QueryInterface
	Limit(count int64) QueryInterface

	OutE(key ...string) QueryInterface
	InE(key ...string) QueryInterface

	As(label string) QueryInterface
	Select(labels []string) QueryInterface
	Values(labels []string) QueryInterface

	GroupCount(label string) QueryInterface

	//code based functions
	Import(source string) QueryInterface
	Map(function string) QueryInterface
	Fold(function string) QueryInterface
	Filter(function string) QueryInterface

	Execute(context.Context) chan aql.ResultRow
	First(context.Context) (aql.ResultRow, error) //Only get one result
	Run(context.Context) error                    //Do execute, but throw away the results
}

type ArachneInterface interface {
	Close()
	AddGraph(string) error
	DeleteGraph(string) error
	GetGraphs() []string
	Query(string) QueryInterface
	Graph(string) DBI
}

type GraphDB interface {
	Query() QueryInterface

	GetVertex(key string, load bool) *aql.Vertex
	GetEdge(key string, load bool) *aql.Edge
	GetBundle(key string, load bool) *aql.Bundle

	GetVertexList(ctx context.Context, load bool) chan aql.Vertex
	GetEdgeList(ctx context.Context, load bool) chan aql.Edge

	GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex

	GetOutList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Vertex
	GetInList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Vertex

	GetOutEdgeList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Edge
	GetInEdgeList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Edge

	SetVertex(vertex aql.Vertex) error
	SetEdge(edge aql.Edge) error
	SetBundle(edge aql.Bundle) error

	DelVertex(key string) error
	DelEdge(key string) error
	DelBundle(id string) error
}

type Indexer interface {
	VertexLabelScan(ctx context.Context, label string) chan string
	EdgeLabelScan(ctx context.Context, label string) chan string
}

type DBI interface {
	GraphDB
	Indexer
}

type Traveler struct {
	State map[string]aql.QueryResult
}

type EdgeFilter func(edge aql.Edge) bool

type PipeRequest struct {
	LoadProperties bool
}
