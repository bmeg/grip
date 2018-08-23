/*
Core Graph Database interfaces
*/

package gdbi

import (
	"context"

	"github.com/bmeg/grip/aql"
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

// Traveler is a query element that traverse the graph
type Traveler struct {
	current      *DataElement
	marks        map[string]*DataElement
	Selections   map[string]*DataElement
	Aggregations map[string]*aql.AggregationResult
	Count        uint32
	Render       interface{}
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
)

// ElementLookup request to look up data
type ElementLookup struct {
	ID     string
	Ref    interface{}
	Vertex *aql.Vertex
	Edge   *aql.Edge
}

// GraphDB is the base interface for graph databases
type GraphDB interface {
	AddGraph(string) error
	DeleteGraph(string) error
	ListGraphs() []string
	Graph(graphID string) (GraphInterface, error)
	GetSchema(ctx context.Context, graphID string, sampleN uint32) (*aql.GraphSchema, error)
	Close() error
}

// GraphInterface is the base Graph data storage interface, the PipeEngine will be able
// to run queries on a data system backend that implements this interface
type GraphInterface interface {
	Compiler() Compiler

	GetTimestamp() string

	GetVertex(key string, load bool) *aql.Vertex
	GetEdge(key string, load bool) *aql.Edge

	AddVertex(vertex []*aql.Vertex) error
	AddEdge(edge []*aql.Edge) error

	DelVertex(key string) error
	DelEdge(key string) error

	VertexLabelScan(ctx context.Context, label string) chan string
	//EdgeLabelScan(ctx context.Context, label string) chan string

	AddVertexIndex(label string, field string) error
	DeleteVertexIndex(label string, field string) error
	GetVertexIndexList() chan aql.IndexID

	GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*aql.AggregationResult, error)
	GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*aql.AggregationResult, error)
	GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*aql.AggregationResult, error)

	GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge

	GetVertexChannel(req chan ElementLookup, load bool) chan ElementLookup
	GetOutChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetOutEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
}

// Manager is a resource manager that is passed to processors to allow them ]
// to make resource requests
type Manager interface {
	//Get handle to temporary KeyValue store driver
	GetTempKV() kvi.KVInterface
	Cleanup()
}

// Compiler takes a aql query and turns it into an executable pipeline
type Compiler interface {
	Compile(stmts []*aql.GraphStatement) (Pipeline, error)
}

// Processor is the interface for a step in the pipe engine
type Processor interface {
	Process(ctx context.Context, man Manager, in InPipe, out OutPipe) context.Context
}

// Pipeline represents a set of processors
type Pipeline interface {
	Processors() []Processor
	DataType() DataType
	MarkTypes() map[string]DataType
}
