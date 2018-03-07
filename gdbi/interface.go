package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
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
	Bundle   map[string]DataElement
	Row      []DataElement
}

// Traveler is a query element that traverse the graph
type Traveler struct {
	current     *DataElement
	marks       map[string]*DataElement
	Count       int64
	GroupCounts map[string]int64
	value       interface{}
}

// DataType is a possible output data type
type DataType uint8

// DataTypes
const (
	NoData DataType = iota
	VertexData
	EdgeData
	CountData
	GroupCountData
	ValueData
	RowData
)

// Processor is the interface for a step in the pipe engine
type Processor interface {
	//DataType() DataType
	Process(in InPipe, out OutPipe)
}

// QueryInterface defines the query engine interface. The primary implementation
// is PipeEngine
/*
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

	Process(in InPipe, out OutPipe)
}
*/

// ElementLookup request to look up data
type ElementLookup struct {
	ID     string
	Ref    interface{}
	Vertex *aql.Vertex
	Edge   *aql.Edge
	Bundle *aql.Bundle
}

// GraphDB is the base interface for graph databases
type GraphDB interface {
	AddGraph(string) error
	DeleteGraph(string) error
	GetGraphs() []string
	Graph(id string) GraphInterface

	Close()
}

// GraphInterface is the base Graph data storage interface, the PipeEngine will be able
// to run queries on a data system backend that implements this interface
type GraphInterface interface {
	GetTimestamp() string

	//Query() QueryInterface

	GetVertex(key string, load bool) *aql.Vertex
	GetEdge(key string, load bool) *aql.Edge
	GetBundle(key string, load bool) *aql.Bundle

	AddVertex(vertex []*aql.Vertex) error
	AddEdge(edge []*aql.Edge) error
	AddBundle(bundle *aql.Bundle) error

	DelVertex(key string) error
	DelEdge(key string) error
	DelBundle(id string) error
	GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge

	GetVertexChannel(req chan ElementLookup, load bool) chan ElementLookup
	GetOutChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetOutEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup

	GetOutBundleChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup

	Indexer
}

// Indexer implements features related to field and value indexing for faster
// subselection of elements before doing traversal
type Indexer interface {
	VertexLabelScan(ctx context.Context, label string) chan string
	EdgeLabelScan(ctx context.Context, label string) chan string
}
