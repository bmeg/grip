package table

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// Mapping describes how table collections become graph vertices and edges.
type Mapping struct {
	Vertices []VertexTable `json:"vertices"`
	Edges    []EdgeTable   `json:"edges"`
}

// VertexTable maps a collection to a vertex label.
type VertexTable struct {
	Collection string `json:"collection"`
	Label      string `json:"label"`
	Prefix     string `json:"prefix"`
	ID         string `json:"id"`
	LabelField string `json:"labelField,omitempty"`
	LabelValue string `json:"labelValue,omitempty"`
}

// EdgeTable maps a collection to a graph edge label.
type EdgeTable struct {
	Collection string   `json:"collection"`
	Label      string   `json:"label"`
	From       Endpoint `json:"from"`
	To         Endpoint `json:"to"`
	LabelField string   `json:"labelField,omitempty"`
	LabelValue string   `json:"labelValue,omitempty"`
}

// Endpoint identifies an endpoint field and the prefix added to its value.
type Endpoint struct {
	Field  string `json:"field"`
	Prefix string `json:"prefix"`
}

type edgeRecord struct {
	edge       gdbi.Edge
	collection string
	rowID      string
}

var _ gdbi.GraphInterface = (*Graph)(nil)

// Graph implements gdbi.GraphInterface over a TableDriver.
// It loads mapped rows once at construction and keeps an in-memory index for
// point lookups and adjacency. It is read-only unless the driver implements
// WriteTableDriver.
type Graph struct {
	driver  TableDriver
	mapping Mapping

	mu           sync.RWMutex
	vertices     map[string]*gdbi.Vertex
	edges        map[string]*edgeRecord
	outEdges     map[string][]*edgeRecord
	inEdges      map[string][]*edgeRecord
	vertexLabels map[string]bool
	edgeLabels   map[string]bool
}

// NewGraph validates the mapping, scans mapped collections, and returns a
// table-backed gdbi.GraphInterface.
func NewGraph(ctx context.Context, driver TableDriver, mapping Mapping) (*Graph, error) {
	if driver == nil {
		return nil, fmt.Errorf("%w: table driver is nil", ErrInvalidRequest)
	}
	if !driver.Capabilities().Readable {
		return nil, fmt.Errorf("%w: table driver is not readable", ErrUnsupported)
	}
	if err := ValidateMapping(mapping); err != nil {
		return nil, err
	}

	g := &Graph{
		driver:       driver,
		mapping:      mapping,
		vertices:     map[string]*gdbi.Vertex{},
		edges:        map[string]*edgeRecord{},
		outEdges:     map[string][]*edgeRecord{},
		inEdges:      map[string][]*edgeRecord{},
		vertexLabels: map[string]bool{},
		edgeLabels:   map[string]bool{},
	}

	for _, vt := range mapping.Vertices {
		stream, err := driver.Scan(ctx, vt.Collection, ScanOptions{})
		if err != nil {
			return nil, fmt.Errorf("scan vertex collection %q: %w", vt.Collection, err)
		}
		for result := range stream {
			if result.Err != nil {
				return nil, fmt.Errorf("scan vertex collection %q: %w", vt.Collection, result.Err)
			}
			if err := ValidateRow(result.Row); err != nil {
				return nil, err
			}
			if vt.LabelField != "" && textValue(result.Row.Data[vt.LabelField]) != vt.LabelValue {
				continue
			}
			key := textValue(result.Row.Data[vt.ID])
			if key == "" {
				key = result.Row.ID
			}
			if key == "" {
				return nil, fmt.Errorf("%w: vertex row %q has no ID field %q", ErrMalformedRow, result.Row.ID, vt.ID)
			}
			id := vt.Prefix + key
			g.vertices[id] = &gdbi.Vertex{
				ID: id, Label: vt.Label, Data: cloneData(result.Row.Data), Loaded: true,
			}
			g.vertexLabels[vt.Label] = true
		}
	}

	for _, et := range mapping.Edges {
		stream, err := driver.Scan(ctx, et.Collection, ScanOptions{})
		if err != nil {
			return nil, fmt.Errorf("scan edge collection %q: %w", et.Collection, err)
		}
		for result := range stream {
			if result.Err != nil {
				return nil, fmt.Errorf("scan edge collection %q: %w", et.Collection, result.Err)
			}
			if err := ValidateRow(result.Row); err != nil {
				return nil, err
			}
			if et.LabelField != "" && textValue(result.Row.Data[et.LabelField]) != et.LabelValue {
				continue
			}
			fromValue := textValue(result.Row.Data[et.From.Field])
			toValue := textValue(result.Row.Data[et.To.Field])
			if fromValue == "" || toValue == "" {
				continue
			}
			fromID := et.From.Prefix + fromValue
			toID := et.To.Prefix + toValue
			id := fromID + "-" + et.Label + "-" + toID
			rec := &edgeRecord{
				edge: gdbi.Edge{
					ID: id, Label: et.Label, From: fromID, To: toID,
					Data: cloneData(result.Row.Data), Loaded: true,
				},
				collection: et.Collection,
				rowID:      result.Row.ID,
			}
			g.edges[id] = rec
			g.outEdges[fromID] = append(g.outEdges[fromID], rec)
			g.inEdges[toID] = append(g.inEdges[toID], rec)
			g.edgeLabels[et.Label] = true
		}
	}

	return g, ctx.Err()
}

// ValidateMapping verifies table graph mapping declarations.
func ValidateMapping(mapping Mapping) error {
	if len(mapping.Vertices) == 0 && len(mapping.Edges) == 0 {
		return fmt.Errorf("%w: table mapping has no vertices or edges", ErrInvalidRequest)
	}
	for _, vt := range mapping.Vertices {
		if vt.Collection == "" || vt.Label == "" || vt.ID == "" {
			return fmt.Errorf("%w: vertex mapping requires collection, label, and id", ErrInvalidRequest)
		}
		if (vt.LabelField == "") != (vt.LabelValue == "") {
			return fmt.Errorf("%w: vertex label field and value must be configured together", ErrInvalidRequest)
		}
	}
	for _, et := range mapping.Edges {
		if et.Collection == "" || et.Label == "" || et.From.Field == "" || et.To.Field == "" {
			return fmt.Errorf("%w: edge mapping requires collection, label, and endpoint fields", ErrInvalidRequest)
		}
		if (et.LabelField == "") != (et.LabelValue == "") {
			return fmt.Errorf("%w: edge label field and value must be configured together", ErrInvalidRequest)
		}
	}
	return nil
}

func textValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

func cloneData(data map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(data))
	for key, value := range data {
		out[key] = value
	}
	return out
}

func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g)
}

func (g *Graph) GetTimestamp() string {
	return "table"
}

func (g *Graph) GetVertex(key string, load bool) *gdbi.Vertex {
	g.mu.RLock()
	vertex, ok := g.vertices[key]
	g.mu.RUnlock()
	if !ok {
		return nil
	}
	if !load {
		return &gdbi.Vertex{ID: vertex.ID, Label: vertex.Label, Loaded: false}
	}
	return &gdbi.Vertex{
		ID: vertex.ID, Label: vertex.Label, Data: cloneData(vertex.Data), Loaded: true,
	}
}

func (g *Graph) GetEdge(key string, load bool) *gdbi.Edge {
	g.mu.RLock()
	record, ok := g.edges[key]
	g.mu.RUnlock()
	if !ok {
		return nil
	}
	edge := record.edge
	if !load {
		return &gdbi.Edge{ID: edge.ID, Label: edge.Label, From: edge.From, To: edge.To, Loaded: false}
	}
	edge.Data = cloneData(edge.Data)
	return &edge
}

func (g *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	writer, ok := g.driver.(WriteTableDriver)
	if !ok || !g.driver.Capabilities().Writable {
		return fmt.Errorf("%w: vertex mutation", ErrUnsupported)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, vertex := range vertices {
		mapping, key, err := g.vertexMapping(vertex)
		if err != nil {
			return err
		}
		data := cloneData(vertex.Data)
		data[mapping.ID] = key
		if err := writer.PutRow(context.Background(), mapping.Collection, &Row{
			ID: key, Data: data,
		}); err != nil {
			return err
		}
		g.vertices[vertex.ID] = &gdbi.Vertex{
			ID: vertex.ID, Label: vertex.Label, Data: cloneData(data), Loaded: true,
		}
		g.vertexLabels[mapping.Label] = true
	}
	return nil
}

func (g *Graph) AddEdge(edges []*gdbi.Edge) error {
	writer, ok := g.driver.(WriteTableDriver)
	if !ok || !g.driver.Capabilities().Writable {
		return fmt.Errorf("%w: edge mutation", ErrUnsupported)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, edge := range edges {
		mapping := g.edgeMapping(edge)
		if mapping == nil {
			return fmt.Errorf("%w: no edge mapping for label %q", ErrInvalidRequest, edge.Label)
		}
		data := cloneData(edge.Data)
		data[mapping.From.Field] = strings.TrimPrefix(edge.From, mapping.From.Prefix)
		data[mapping.To.Field] = strings.TrimPrefix(edge.To, mapping.To.Prefix)
		rowID := edge.ID
		if err := writer.PutRow(context.Background(), mapping.Collection, &Row{ID: rowID, Data: data}); err != nil {
			return err
		}
		rec := &edgeRecord{
			edge: gdbi.Edge{
				ID: edge.ID, Label: edge.Label, From: edge.From, To: edge.To,
				Data: cloneData(data), Loaded: true,
			},
			collection: mapping.Collection,
			rowID:      rowID,
		}
		g.edges[edge.ID] = rec
		g.outEdges[edge.From] = append(g.outEdges[edge.From], rec)
		g.inEdges[edge.To] = append(g.inEdges[edge.To], rec)
		g.edgeLabels[edge.Label] = true
	}
	return nil
}

func (g *Graph) vertexMapping(vertex *gdbi.Vertex) (VertexTable, string, error) {
	for _, vt := range g.mapping.Vertices {
		if vt.Label == vertex.Label && strings.HasPrefix(vertex.ID, vt.Prefix) {
			return vt, strings.TrimPrefix(vertex.ID, vt.Prefix), nil
		}
	}
	return VertexTable{}, "", fmt.Errorf("%w: no vertex mapping for %q", ErrInvalidRequest, vertex.ID)
}

func (g *Graph) edgeMapping(edge *gdbi.Edge) *EdgeTable {
	for i := range g.mapping.Edges {
		mapping := &g.mapping.Edges[i]
		if mapping.Label == edge.Label &&
			strings.HasPrefix(edge.From, mapping.From.Prefix) &&
			strings.HasPrefix(edge.To, mapping.To.Prefix) {
			return mapping
		}
	}
	return nil
}

func (g *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	for element := range stream {
		if element.Vertex != nil {
			if err := g.AddVertex([]*gdbi.Vertex{element.Vertex}); err != nil {
				return err
			}
		}
		if element.Edge != nil {
			if err := g.AddEdge([]*gdbi.Edge{element.Edge}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Graph) BulkDel(data *gdbi.DeleteData) error {
	for _, key := range data.Vertices {
		if err := g.DelVertex(key); err != nil {
			return err
		}
	}
	for _, key := range data.Edges {
		if err := g.DelEdge(key); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) DelVertex(key string) error {
	writer, writable := g.driver.(WriteTableDriver)
	if !writable || !g.driver.Capabilities().Writable {
		return fmt.Errorf("%w: vertex mutation", ErrUnsupported)
	}
	g.mu.Lock()
	vertex, ok := g.vertices[key]
	if ok {
		delete(g.vertices, key)
	}
	g.mu.Unlock()
	if ok {
		for _, vt := range g.mapping.Vertices {
			if strings.HasPrefix(key, vt.Prefix) && (!ok || vt.Label == vertex.Label) {
				return writer.DeleteRow(context.Background(), vt.Collection, strings.TrimPrefix(key, vt.Prefix))
			}
		}
	}
	return nil
}

func (g *Graph) DelEdge(key string) error {
	writer, writable := g.driver.(WriteTableDriver)
	if !writable || !g.driver.Capabilities().Writable {
		return fmt.Errorf("%w: edge mutation", ErrUnsupported)
	}
	g.mu.Lock()
	record, ok := g.edges[key]
	if ok {
		delete(g.edges, key)
		g.outEdges[record.edge.From] = removeEdge(g.outEdges[record.edge.From], record)
		g.inEdges[record.edge.To] = removeEdge(g.inEdges[record.edge.To], record)
	}
	g.mu.Unlock()
	if !ok {
		return nil
	}
	return writer.DeleteRow(context.Background(), record.collection, record.rowID)
}

func removeEdge(edges []*edgeRecord, target *edgeRecord) []*edgeRecord {
	out := edges[:0]
	for _, edge := range edges {
		if edge != target {
			out = append(out, edge)
		}
	}
	return out
}

func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		if ctx.Err() != nil {
			return
		}
		g.mu.RLock()
		ids := make([]string, 0)
		for id, vertex := range g.vertices {
			if label == "" || vertex.Label == label {
				ids = append(ids, id)
			}
		}
		g.mu.RUnlock()
		sort.Strings(ids)
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- id:
			}
		}
	}()
	return out
}

func (g *Graph) ListVertexLabels() ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	labels := make([]string, 0, len(g.vertexLabels))
	for label := range g.vertexLabels {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels, nil
}

func (g *Graph) ListEdgeLabels() ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	labels := make([]string, 0, len(g.edgeLabels))
	for label := range g.edgeLabels {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels, nil
}

func (g *Graph) AddVertexIndex(string, string) error {
	return fmt.Errorf("%w: vertex index mutation", ErrUnsupported)
}

func (g *Graph) DeleteVertexIndex(string, string) error {
	return fmt.Errorf("%w: vertex index mutation", ErrUnsupported)
}

func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	out := make(chan *gripql.IndexID)
	close(out)
	return out
}

func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	out := make(chan *gdbi.Vertex)
	go func() {
		defer close(out)
		g.mu.RLock()
		vertices := make([]*gdbi.Vertex, 0, len(g.vertices))
		for _, vertex := range g.vertices {
			if load {
				vertices = append(vertices, &gdbi.Vertex{
					ID: vertex.ID, Label: vertex.Label, Data: cloneData(vertex.Data), Loaded: true,
				})
			} else {
				vertices = append(vertices, &gdbi.Vertex{
					ID: vertex.ID, Label: vertex.Label, Loaded: false,
				})
			}
		}
		g.mu.RUnlock()
		sort.Slice(vertices, func(i, j int) bool { return vertices[i].ID < vertices[j].ID })
		for _, vertex := range vertices {
			select {
			case <-ctx.Done():
				return
			case out <- vertex:
			}
		}
	}()
	return out
}

func (g *Graph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup)
	go func() {
		defer close(out)
		for lookup := range req {
			if lookup.IsSignal() {
				sendLookup(ctx, out, lookup)
				continue
			}
			if lookup.ID == "" {
				continue
			}
			vertex := g.GetVertex(lookup.ID, load)
			if vertex == nil {
				continue
			}
			lookup.Vertex = vertex
			sendLookup(ctx, out, lookup)
		}
	}()
	return out
}

func sendLookup(ctx context.Context, out chan<- gdbi.ElementLookup, lookup gdbi.ElementLookup) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case out <- lookup:
		return true
	}
}

func (g *Graph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup)
	go func() {
		defer close(out)
		for lookup := range req {
			if lookup.IsSignal() {
				sendLookup(ctx, out, lookup)
				continue
			}
			found := false
			g.mu.RLock()
			edges := append([]*edgeRecord(nil), g.outEdges[lookup.ID]...)
			g.mu.RUnlock()
			for _, record := range edges {
				if len(edgeLabels) > 0 && !containsString(edgeLabels, record.edge.Label) {
					continue
				}
				found = true
				vertex := g.GetVertex(record.edge.To, load)
				if vertex != nil {
					next := lookup
					next.Vertex = vertex
					sendLookup(ctx, out, next)
				}
			}
			if emitNull && !found {
				next := lookup
				next.Vertex = nil
				sendLookup(ctx, out, next)
			}
		}
	}()
	return out
}

func (g *Graph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup)
	go func() {
		defer close(out)
		for lookup := range req {
			if lookup.IsSignal() {
				sendLookup(ctx, out, lookup)
				continue
			}
			found := false
			g.mu.RLock()
			edges := append([]*edgeRecord(nil), g.inEdges[lookup.ID]...)
			g.mu.RUnlock()
			for _, record := range edges {
				if len(edgeLabels) > 0 && !containsString(edgeLabels, record.edge.Label) {
					continue
				}
				found = true
				vertex := g.GetVertex(record.edge.From, load)
				if vertex != nil {
					next := lookup
					next.Vertex = vertex
					sendLookup(ctx, out, next)
				}
			}
			if emitNull && !found {
				next := lookup
				next.Vertex = nil
				sendLookup(ctx, out, next)
			}
		}
	}()
	return out
}

func (g *Graph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup)
	go func() {
		defer close(out)
		for lookup := range req {
			if lookup.IsSignal() {
				sendLookup(ctx, out, lookup)
				continue
			}
			found := false
			g.mu.RLock()
			edges := append([]*edgeRecord(nil), g.outEdges[lookup.ID]...)
			g.mu.RUnlock()
			for _, record := range edges {
				if len(edgeLabels) > 0 && !containsString(edgeLabels, record.edge.Label) {
					continue
				}
				found = true
				next := lookup
				next.Edge = g.GetEdge(record.edge.ID, load)
				sendLookup(ctx, out, next)
			}
			if emitNull && !found {
				next := lookup
				next.Edge = nil
				sendLookup(ctx, out, next)
			}
		}
	}()
	return out
}

func (g *Graph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup)
	go func() {
		defer close(out)
		for lookup := range req {
			if lookup.IsSignal() {
				sendLookup(ctx, out, lookup)
				continue
			}
			found := false
			g.mu.RLock()
			edges := append([]*edgeRecord(nil), g.inEdges[lookup.ID]...)
			g.mu.RUnlock()
			for _, record := range edges {
				if len(edgeLabels) > 0 && !containsString(edgeLabels, record.edge.Label) {
					continue
				}
				found = true
				next := lookup
				next.Edge = g.GetEdge(record.edge.ID, load)
				sendLookup(ctx, out, next)
			}
			if emitNull && !found {
				next := lookup
				next.Edge = nil
				sendLookup(ctx, out, next)
			}
		}
	}()
	return out
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
