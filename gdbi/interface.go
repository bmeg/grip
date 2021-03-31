/*
Core Graph Database interfaces
*/

package gdbi

import (
	"context"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
)

// InPipe incoming traveler messages
type InPipe <-chan *Traveler

// OutPipe collects output traveler messages
type OutPipe chan<- *Traveler

// DataElement is a single data element
type DataElement struct {
	ID       string
	Label    string
	From, To string
	Data     map[string]interface{}
}

type Aggregate struct {
	Name  string
	Key   interface{}
	Value float64
}

type DataElementID struct {
	Vertex string
	Edge   string
}

// Traveler is a query element that traverse the graph
type Traveler struct {
	current     *DataElement
	marks       map[string]*DataElement
	Selections  map[string]*DataElement
	Aggregation *Aggregate
	Count       uint32
	Render      interface{}
	Path        []DataElementID
}

// DataType is a possible output data type
type DataType uint8

// DataTypes
const (
	NoData DataType = iota
	VertexData
	EdgeData
	CountData
	AggregationData
	SelectionData
	RenderData
	PathData
)

// ElementLookup request to look up data
type ElementLookup struct {
	ID     string
	Ref    *Traveler
	Vertex *gripql.Vertex
	Edge   *gripql.Edge
}

// GraphDB is the base interface for graph databases
type GraphDB interface {
	AddGraph(string) error
	DeleteGraph(string) error
	ListGraphs() []string
	Graph(graphID string) (GraphInterface, error)
	BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error)
	Close() error
}

// GraphInterface is the base Graph data storage interface, the PipeEngine will be able
// to run queries on a data system backend that implements this interface
type GraphInterface interface {
	Compiler() Compiler

	GetTimestamp() string

	GetVertex(key string, load bool) *gripql.Vertex
	GetEdge(key string, load bool) *gripql.Edge

	AddVertex(vertex []*gripql.Vertex) error
	AddEdge(edge []*gripql.Edge) error

	BulkAdd(<-chan *gripql.GraphElement) error

	DelVertex(key string) error
	DelEdge(key string) error

	VertexLabelScan(ctx context.Context, label string) chan string
	// EdgeLabelScan(ctx context.Context, label string) chan string
	ListVertexLabels() ([]string, error)
	ListEdgeLabels() ([]string, error)

	AddVertexIndex(label string, field string) error
	DeleteVertexIndex(label string, field string) error
	GetVertexIndexList() <-chan *gripql.IndexID

	GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge

	GetVertexChannel(ctx context.Context, req chan ElementLookup, load bool) chan ElementLookup
	GetOutChannel(ctx context.Context, req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInChannel(ctx context.Context, req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetOutEdgeChannel(ctx context.Context, req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInEdgeChannel(ctx context.Context, req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
}

// Manager is a resource manager that is passed to processors to allow them ]
// to make resource requests
type Manager interface {
	//Get handle to temporary KeyValue store driver
	GetTempKV() kvi.KVInterface
	Cleanup()
}
