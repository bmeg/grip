package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
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

	GroupCount(label string) QueryInterface

	//code based functions
	Import(source string) QueryInterface
	Map(function string) QueryInterface
	Fold(function string) QueryInterface

	Execute(context.Context) chan aql.ResultRow
	First(context.Context) (aql.ResultRow, error) //Only get one result
	Run(context.Context) error                    //Do execute, but throw away the results
}

type ArachneInterface interface {
	Close()
	Query(string) QueryInterface
	Graph(string) DBI
}

type DBI interface {
	Query() QueryInterface

	GetVertex(key string, load bool) *aql.Vertex
	GetVertexList(ctx context.Context, load bool) chan aql.Vertex
	GetEdgeList(ctx context.Context, load bool) chan aql.Edge

	GetOutList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Vertex
	GetInList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Vertex

	GetOutEdgeList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Edge
	GetInEdgeList(ctx context.Context, key string, load bool, filter EdgeFilter) chan aql.Edge

	DelVertex(key string) error
	DelEdge(key string) error
	SetVertex(vertex aql.Vertex) error
	SetEdge(edge aql.Edge) error
	//SetEdgeBundle(edge aql.EdgeBundle) error
}

type Traveler struct {
	State map[string]aql.QueryResult
}

type EdgeFilter func(edge aql.Edge) bool

type PipeRequest struct {
	LoadProperties bool
}

type GraphPipe func(ctx context.Context) chan Traveler
