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
type InPipe <-chan Traveler

// OutPipe collects output traveler messages
type OutPipe chan<- Traveler

// DataElement is a single data element
type DataElement struct {
	ID       string
	Label    string
	From, To string
	Data     map[string]interface{}
	Loaded   bool
}

// DataRef is a handler interface above DataElement, that allows processing pipelines
// to avoid loading data data required for DataElement until it is actually needed
type DataRef interface {
	Get() *DataElement
	Copy() DataRef
	ToDict() map[string]any
}

func (d *DataElement) Get() *DataElement {
	return d
}

func (d *DataElement) Copy() DataRef {
	return &DataElement{
		ID:     d.ID,
		To:     d.To,
		From:   d.From,
		Label:  d.Label,
		Loaded: d.Loaded,
		Data:   d.Data,
	}
}

type Vertex = DataElement
type Edge = DataElement

type VertexRef = DataRef
type EdgeRef = DataRef

type GraphElement struct {
	Vertex *Vertex
	Edge   *Edge
	Graph  string
}

type DeleteData struct {
	Graph    string
	Vertices []string
	Edges    []string
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

type Signal struct {
	Dest string
	ID   int
}

// Traveler is a query element that traverse the graph
type BaseTraveler struct {
	Current     *DataElement
	Marks       map[string]*DataElement
	Selections  map[string]*DataElement
	Aggregation *Aggregate
	Count       uint32
	Render      interface{}
	Path        []DataElementID
	Signal      *Signal
}

type Traveler interface {
	IsSignal() bool
	GetSignal() Signal
	IsNull() bool
	GetCurrent() DataRef
	GetCurrentID() string
	AddCurrent(r DataRef) Traveler
	Copy() Traveler
	HasMark(label string) bool
	GetMark(label string) DataRef
	// AddMark adds a new mark to the data and return a duplicated Traveler
	AddMark(label string, r DataRef) Traveler
	// UpdateMark changes the data of a mark in the original traveler (vs AddMark which changes a copy of the traveler)
	UpdateMark(label string, r DataRef)
	ListMarks() []string
	GetSelections() map[string]DataRef
	GetRender() interface{}
	GetPath() []DataElementID
	GetAggregation() *Aggregate
	GetCount() uint32
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
	Ref    Traveler
	Vertex VertexRef
	Edge   EdgeRef
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

	GetVertex(key string, load bool) *Vertex
	GetEdge(key string, load bool) *Edge

	AddVertex(vertex []*Vertex) error
	AddEdge(edge []*Edge) error

	BulkAdd(<-chan *GraphElement) error
	BulkDel(*DeleteData) error

	DelVertex(key string) error
	DelEdge(key string) error

	VertexLabelScan(ctx context.Context, label string) chan string
	// EdgeLabelScan(ctx context.Context, label string) chan string
	ListVertexLabels() ([]string, error)
	ListEdgeLabels() ([]string, error)

	AddVertexIndex(label string, field string) error
	DeleteVertexIndex(label string, field string) error
	GetVertexIndexList() <-chan *gripql.IndexID

	GetVertexList(ctx context.Context, load bool) <-chan *Vertex
	GetEdgeList(ctx context.Context, load bool) <-chan *Edge

	GetVertexChannel(ctx context.Context, req chan ElementLookup, load bool) chan ElementLookup
	GetOutChannel(ctx context.Context, req chan ElementLookup, load bool, emitNull bool, edgeLabels []string) chan ElementLookup
	GetInChannel(ctx context.Context, req chan ElementLookup, load bool, emitNull bool, edgeLabels []string) chan ElementLookup
	GetOutEdgeChannel(ctx context.Context, req chan ElementLookup, load bool, emitNull bool, edgeLabels []string) chan ElementLookup
	GetInEdgeChannel(ctx context.Context, req chan ElementLookup, load bool, emitNull bool, edgeLabels []string) chan ElementLookup
}

// Manager is a resource manager that is passed to processors to allow them ]
// to make resource requests
type Manager interface {
	//Get handle to temporary KeyValue store driver
	GetTempKV() kvi.KVInterface
	Cleanup()
}
