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

// Row is a single row of data, which may be in different states of materialization
type Row interface {
	GetID() string
	GetLabel() string
	GetFrom() string
	GetTo() string
	Mode() RowMode
	GetPayload() map[string]any // The user-defined data map
	GetRaw() string             // The raw JSON string if available
	IsLoaded() bool             // Whether the payload is already in memory
	Copy() Row
	ToDict() map[string]any // The full document including system fields (_id, _label)
}

type RowMode uint8

const (
	RowModeUnknown RowMode = iota
	RowModeReference
	RowModeRaw
	RowModeProjected
	RowModeMaterialized
)

// DataElement is a materialized row of data
type DataElement struct {
	ID       string                 `json:"id"`
	Label    string                 `json:"label"`
	From     string                 `json:"from,omitempty"`
	To       string                 `json:"to,omitempty"`
	Data     map[string]interface{} `json:"data"`
	RawJSON  string                 `json:"-"`
	Loaded   bool                   `json:"-"`
	Mutable  bool                   `json:"-"`
	ModeHint RowMode                `json:"-"`
}

func (d *DataElement) GetID() string    { return d.ID }
func (d *DataElement) GetLabel() string { return d.Label }
func (d *DataElement) GetFrom() string  { return d.From }
func (d *DataElement) GetTo() string    { return d.To }
func (d *DataElement) GetRaw() string   { return d.RawJSON }
func (d *DataElement) IsLoaded() bool   { return d.Loaded }
func (d *DataElement) IsNilRow() bool   { return d == nil }
func (d *DataElement) Mode() RowMode {
	if d == nil {
		return RowModeUnknown
	}
	if d.ModeHint != RowModeUnknown {
		return d.ModeHint
	}
	if d.RawJSON != "" && d.Data == nil {
		return RowModeRaw
	}
	if d.Data != nil {
		if !d.Loaded && d.RawJSON == "" && len(d.Data) == 0 {
			return RowModeReference
		}
		if d.Loaded {
			return RowModeMaterialized
		}
		return RowModeProjected
	}
	if !d.Loaded && d.RawJSON == "" {
		return RowModeReference
	}
	return RowModeUnknown
}
func (d *DataElement) GetPayload() map[string]any {
	d.materializeRawData()
	return d.Data
}
func (d *DataElement) Copy() Row {
	if d == nil {
		return (*DataElement)(nil)
	}
	// Keep raw rows raw; materialization should happen only when payload data is requested.
	if d.RawJSON != "" && d.Data == nil {
		return &DataElement{
			ID: d.ID, To: d.To, From: d.From, Label: d.Label,
			Loaded: d.Loaded, Mutable: d.Mutable, ModeHint: d.ModeHint, Data: nil, RawJSON: d.RawJSON,
		}
	}
	var newData map[string]any
	if d.Data != nil {
		if d.Mutable {
			newData = make(map[string]any, len(d.Data))
			for k, v := range d.Data {
				newData[k] = v
			}
		} else {
			// Immutable payloads are shared until a mutator requests write access.
			newData = d.Data
		}
	}
	return &DataElement{
		ID: d.ID, To: d.To, From: d.From, Label: d.Label,
		Loaded: d.Loaded, Mutable: d.Mutable, ModeHint: d.ModeHint, Data: newData, RawJSON: d.RawJSON,
	}
}

type Vertex = DataElement
type Edge = DataElement

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
	Current     Row
	Marks       map[string]Row
	Selections  map[string]Row
	Aggregation *Aggregate
	Count       uint32
	Render      interface{}
	Path        []DataElementID
	TrackPath   bool
	Signal      *Signal
}

type Traveler interface {
	IsSignal() bool
	GetSignal() Signal
	IsNull() bool
	GetCurrent() Row
	GetCurrentID() string
	AddCurrent(r Row) Traveler
	Copy() Traveler
	HasMark(label string) bool
	GetMark(label string) Row
	// AddMark adds a new mark to the data and return a duplicated Traveler
	AddMark(label string, r Row) Traveler
	// UpdateMark changes the data of a mark in the original traveler (vs AddMark which changes a copy of the traveler)
	UpdateMark(label string, r Row)
	ListMarks() []string
	GetSelections() map[string]Row
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
	Vertex *DataElement
	Edge   *DataElement
	Meta   LookupMeta
}

// LookupMeta is the canonical execution metadata carried between lookup and traversal stages.
// Opaque keeps driver-specific location metadata while the rest are shared core hints.
type LookupMeta struct {
	Opaque any
	Fields []string
	Data   map[string]any
	UID    uint64
	EUID   uint64
	SUID   uint64
	DUID   uint64
}

func IsLookupMetaEmpty(m LookupMeta) bool {
	return m.Opaque == nil &&
		len(m.Fields) == 0 &&
		len(m.Data) == 0 &&
		m.UID == 0 &&
		m.EUID == 0 &&
		m.SUID == 0 &&
		m.DUID == 0
}

func (e ElementLookup) GetLookupMeta() (LookupMeta, bool) {
	if !IsLookupMetaEmpty(e.Meta) {
		return e.Meta, true
	}
	return LookupMeta{}, false
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
	GetTmpDir() string
	Cleanup()
}
