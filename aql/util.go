package aql

import (
	"io"
	// "log"
	"context"
	"fmt"
	"sort"

	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc"
)

// Client is a GRPC arachne client with some helper functions
type Client struct {
	QueryC QueryClient
	EditC  EditClient
}

// Connect opens a GRPC connection to an Arachne server
func Connect(address string, write bool) (Client, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return Client{}, err
	}
	queryOut := NewQueryClient(conn)
	if !write {
		return Client{queryOut, nil}, err
	}
	editOut := NewEditClient(conn)
	return Client{queryOut, editOut}, err
}

// GetGraphs lists the graphs
func (client Client) GetGraphs() chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		cl, err := client.QueryC.GetGraphs(context.Background(), &Empty{})
		if err != nil {
			return
		}
		for {
			elem, err := cl.Recv()
			if err == io.EOF || err != nil {
				break
			}

			out <- elem.Graph
		}
	}()
	return out
}

// GetGraphList gets graphs from the server, as a list (rather then a channel)
func (client Client) GetGraphList() []string {
	out := []string{}
	for i := range client.GetGraphs() {
		out = append(out, i)
	}
	return out
}

// GetTimestamp get update timestamp for graph
func (client Client) GetTimestamp(graph string) (*Timestamp, error) {
	ts, err := client.QueryC.GetTimestamp(context.Background(), &ElementID{Graph: graph})
	return ts, err
}

// DeleteGraph deletes a graph and all of its contents
func (client Client) DeleteGraph(graph string) error {
	_, err := client.EditC.DeleteGraph(context.Background(), &ElementID{Graph: graph})
	return err
}

// AddGraph creates a new graph
func (client Client) AddGraph(graph string) error {
	_, err := client.EditC.AddGraph(context.Background(), &ElementID{Graph: graph})
	return err
}

// AddVertex adds a single vertex to the graph
func (client Client) AddVertex(graph string, v *Vertex) error {
	_, err := client.EditC.AddVertex(context.Background(), &GraphElement{Graph: graph, Vertex: v})
	return err
}

// AddEdge adds a single edge to the graph
func (client Client) AddEdge(graph string, e *Edge) error {
	_, err := client.EditC.AddEdge(context.Background(), &GraphElement{Graph: graph, Edge: e})
	return err
}

// AddSubGraph adds a complete subgraph to an existing graph
func (client Client) AddSubGraph(graph string, g *Graph) error {
	_, err := client.EditC.AddSubGraph(context.Background(), &Graph{Graph: graph, Edges: g.Edges, Vertices: g.Vertices})
	return err
}

// StreamElements allows for bulk continuous loading of graph elements into the datastore
func (client Client) StreamElements(elemChan chan *GraphElement) error {
	sc, err := client.EditC.StreamElements(context.Background())
	if err != nil {
		return err
	}
	for elem := range elemChan {
		err := sc.Send(elem)
		if err != nil {
			return err
		}
	}
	_, err = sc.CloseAndRecv()
	return err
}

// GetVertex obtains a vertex from a graph by `id`
func (client Client) GetVertex(graph string, id string) (*Vertex, error) {
	v, err := client.QueryC.GetVertex(context.Background(), &ElementID{Graph: graph, Id: id})
	return v, err
}

// Execute executes the given query.
func (client Client) Execute(graph string, q *Query) (chan *ResultRow, error) {
	return client.Traversal(&GraphQuery{
		Graph: graph,
		Query: q.Statements,
	})
}

// Traversal runs a graph traversal query
func (client Client) Traversal(query *GraphQuery) (chan *ResultRow, error) {
	tclient, err := client.QueryC.Traversal(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	out := make(chan *ResultRow, 100)
	go func() {
		defer close(out)
		for t, err := tclient.Recv(); err == nil; t, err = tclient.Recv() {
			out <- t
		}
	}()
	return out, nil
}

// GetDataMap obtains data attached to vertex in the form of a map
func (vertex *Vertex) GetDataMap() map[string]interface{} {
	return protoutil.AsMap(vertex.Data)
}

// SetDataMap obtains data attached to vertex in the form of a map
func (vertex *Vertex) SetDataMap(i map[string]interface{}) {
	vertex.Data = protoutil.AsStruct(i)
}

// SetProperty sets named field in Vertex data
func (vertex *Vertex) SetProperty(key string, value interface{}) {
	if vertex.Data == nil {
		vertex.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	protoutil.StructSet(vertex.Data, key, value)
}

// GetProperty get named field from vertex data
func (vertex *Vertex) GetProperty(key string) interface{} {
	if vertex.Data == nil {
		return nil
	}
	m := protoutil.AsMap(vertex.Data)
	return m[key]
}

// GetProperty get named field from edge data
func (edge *Edge) GetProperty(key string) interface{} {
	if edge.Data == nil {
		return nil
	}
	m := protoutil.AsMap(edge.Data)
	return m[key]
}

// HasProperty returns true is field is defined
func (edge *Edge) HasProperty(key string) bool {
	if edge.Data == nil {
		return false
	}
	m := protoutil.AsMap(edge.Data)
	_, ok := m[key]
	return ok
}

// AsMap converts a NamedAggregationResult to a map[string]interface{}
func (namedAggRes *NamedAggregationResult) AsMap() map[string]interface{} {
	buckets := make([]map[string]interface{}, len(namedAggRes.Buckets))
	for i, b := range namedAggRes.Buckets {
		buckets[i] = b.AsMap()
	}

	return map[string]interface{}{
		"name":    namedAggRes.Name,
		"buckets": buckets,
	}
}

// AsMap converts an AggregationResult to a map[string]interface{}
func (aggRes *AggregationResult) AsMap() map[string]interface{} {
	return map[string]interface{}{
		"key":   aggRes.Key,
		"value": aggRes.Value,
	}
}

// SortedInsert inserts an AggregationResult into the Buckets field
// and returns the index of the insertion
func (namedAggRes *NamedAggregationResult) SortedInsert(el *AggregationResult) (int, error) {
	if !namedAggRes.IsValueSorted() {
		return 0, fmt.Errorf("buckets are not value sorted")
	}

	if len(namedAggRes.Buckets) == 0 {
		namedAggRes.Buckets = []*AggregationResult{el}
		return 0, nil
	}

	index := sort.Search(len(namedAggRes.Buckets), func(i int) bool {
		if namedAggRes.Buckets[i] == nil {
			return true
		}
		return el.Value > namedAggRes.Buckets[i].Value
	})

	namedAggRes.Buckets = append(namedAggRes.Buckets, &AggregationResult{})
	copy(namedAggRes.Buckets[index+1:], namedAggRes.Buckets[index:])
	namedAggRes.Buckets[index] = el

	return index, nil
}

// SortOnValue sorts Buckets by Value in descending order
func (namedAggRes *NamedAggregationResult) SortOnValue() {
	sort.Slice(namedAggRes.Buckets, func(i, j int) bool {
		if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] != nil {
			return true
		}
		if namedAggRes.Buckets[i] != nil && namedAggRes.Buckets[j] == nil {
			return false
		}
		if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] == nil {
			return false
		}
		return namedAggRes.Buckets[i].Value > namedAggRes.Buckets[j].Value
	})
}

// IsValueSorted returns true if the Buckets are sorted by Value
func (namedAggRes *NamedAggregationResult) IsValueSorted() bool {
	for i := range namedAggRes.Buckets {
		j := i + 1
		if i < len(namedAggRes.Buckets)-2 {
			if namedAggRes.Buckets[i] != nil && namedAggRes.Buckets[j] == nil {
				return true
			}
			if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] != nil {
				return false
			}
			if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] == nil {
				return true
			}
			if namedAggRes.Buckets[i].Value < namedAggRes.Buckets[j].Value {
				return false
			}
		}
	}
	return true
}
