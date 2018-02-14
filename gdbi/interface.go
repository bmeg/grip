package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
)

type GraphsDB interface {
	AddGraph(string) error
	DeleteGraph(string) error
	GetGraphs() []string
  Graph(id string) GraphDB
}

type GraphDB interface {

	GetVertex(key string, load bool) *aql.Vertex
	GetEdge(key string, load bool) *aql.Edge
	GetBundle(key string, load bool) *aql.Bundle

	AddVertex(vertex *aql.Vertex) error
	AddEdge(edge *aql.Edge) error
	AddBundle(bundle *aql.Bundle) error

	DelVertex(key string) error
	DelEdge(key string) error
	DelBundle(id string) error
	GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge

	GetVertexListByID(ctx context.Context, ids chan string, load bool) <-chan *aql.Vertex

	GetOutList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Vertex
	GetInList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Vertex

	GetOutEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Edge
	GetInEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Edge

	GetOutBundleList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Bundle

	Indexer
}

// Indexer implements features related to field and value indexing for faster
// subselection of elements before doing traversal
type Indexer interface {
	VertexLabelScan(ctx context.Context, label string) chan string
	EdgeLabelScan(ctx context.Context, label string) chan string
}

// DBI implements the full GraphDB and Indexer interfaces
type DBI interface {
	GraphsDB
	Close()
}
