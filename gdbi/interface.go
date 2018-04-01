package gdbi

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/kvi"
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
	Compiler() Compiler

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

	VertexLabelScan(ctx context.Context, label string) chan string
	//EdgeLabelScan(ctx context.Context, label string) chan string

	AddVertexIndex(label string, field string) error
	//AddEdgeIndex(label string, field string) error

	GetVertexIndexList() chan aql.IndexID

	DeleteVertexIndex(label string, field string) error
	//DeleteEdgeIndex(label string, field string) error

	GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount
	//GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount

	GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge

	GetVertexChannel(req chan ElementLookup, load bool) chan ElementLookup
	GetOutChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetOutEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
	GetInEdgeChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup

	GetOutBundleChannel(req chan ElementLookup, load bool, edgeLabels []string) chan ElementLookup
}

type Manager interface {
	//Get handle to temporary KeyValue store driver
	GetTempKV() kvi.KVInterface
	Cleanup()
}

type Compiler interface {
	Compile(stmts []*aql.GraphStatement, workDir string) (Pipeline, error)
}

// Processor is the interface for a step in the pipe engine
type Processor interface {
	Process(ctx context.Context, man Manager, in InPipe, out OutPipe) context.Context
}

type Pipeline interface {
	Processors() []Processor
	DataType() DataType
	RowTypes() []DataType
}

/*
// Pipeline represents the output of a single pipeline chain
type Pipeline interface {
	//StartInput(chan Traveler) error
	Start(ctx context.Context) chan Traveler
	GetCurrentState() int
	GetValueStates() map[string]int
}
*/
