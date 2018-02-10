package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
)

// QueryInterface defines the query engine interface. The primary implementation
// is PipeEngine
type QueryInterface interface {
	V(ids []string) QueryInterface
	E() QueryInterface
	Count() QueryInterface

	Has(prop string, value ...string) QueryInterface
	HasLabel(labels ...string) QueryInterface
	HasID(ids ...string) QueryInterface

	Out(key ...string) QueryInterface
	In(key ...string) QueryInterface
	Both(key ...string) QueryInterface
	Limit(count int64) QueryInterface

	OutE(key ...string) QueryInterface
	InE(key ...string) QueryInterface
	BothE(key ...string) QueryInterface

	OutBundle(key ...string) QueryInterface

	As(label string) QueryInterface
	Select(labels []string) QueryInterface
	Values(labels []string) QueryInterface

	GroupCount(label string) QueryInterface

	//Subqueries
	Match(matches []*QueryInterface) QueryInterface

	//code based functions
	Import(source string) QueryInterface
	Map(function string) QueryInterface
	Fold(function string, init interface{}) QueryInterface
	Filter(function string) QueryInterface
	FilterValues(source string) QueryInterface
	VertexFromValues(source string) QueryInterface

	Execute(context.Context) chan aql.ResultRow
	First(context.Context) (aql.ResultRow, error) //Only get one result
	Run(context.Context) error                    //Do execute, but throw away the results
	Chain(context.Context, PipeOut) PipeOut
}

// ArachneInterface the base graph data storage interface
type ArachneInterface interface {
	Close()
	AddGraph(string) error
	DeleteGraph(string) error
	GetGraphs() []string
	Query(string) QueryInterface
	Graph(string) GraphDB
}

// GraphDB is the base Graph data storage interface, the PipeEngine will be able
// to run queries on a data system backend that implements this interface
type GraphDB interface {
	Query() QueryInterface

	GetVertex(key string, load bool) *aql.Vertex
	GetEdge(key string, load bool) *aql.Edge
	GetBundle(key string, load bool) *aql.Bundle

	GetVertexList(ctx context.Context, load bool) chan aql.Vertex
	GetEdgeList(ctx context.Context, load bool) chan aql.Edge

	GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex

	GetOutList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Vertex
	GetInList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Vertex

	GetOutEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Edge
	GetInEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Edge

	GetOutBundleList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Bundle

	SetVertex(vertex aql.Vertex) error
	SetEdge(edge aql.Edge) error
	SetBundle(edge aql.Bundle) error

	DelVertex(key string) error
	DelEdge(key string) error
	DelBundle(id string) error

	VertexLabelScan(ctx context.Context, label string) chan string
	EdgeLabelScan(ctx context.Context, label string) chan string
}

// These consts mark the type of a PipeOut traveler chan
const (
	// StateCustom The PipeOut will be emitting custom data structures
	StateCustom = 0
	// StateVertexList The PipeOut will be emitting a list of vertices
	StateVertexList = 1
	// StateEdgeList The PipeOut will be emitting a list of edges
	StateEdgeList = 2
	// StateRawVertexList The PipeOut will be emitting a list of all vertices, if there is an index
	// based filter, you can use skip listening and use that
	StateRawVertexList = 3
	// StateRawEdgeList The PipeOut will be emitting a list of all edges, if there is an index
	// based filter, you can use skip listening and use that
	StateRawEdgeList = 4
	// StateBundleList the PipeOut will be emittign a list of bundles
	StateBundleList = 5
)

// PipeOut represents the output of a single pipeline chain
type PipeOut struct {
	Travelers   chan Traveler
	State       int
	ValueStates map[string]int
}

// Traveler represents one query element, tracking progress across the graph
type Traveler struct {
	State map[string]aql.QueryResult
}

/*
type PipeRequest struct {
	LoadProperties bool
}
*/
