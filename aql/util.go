package aql

import (
	"io"
	//"log"
	//"fmt"
	"context"

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
func (client Client) AddVertex(graph string, v Vertex) error {
	client.EditC.AddVertex(context.Background(), &GraphElement{Graph: graph, Vertex: &v})
	return nil
}

// AddEdge adds a single edge to the graph
func (client Client) AddEdge(graph string, e Edge) error {
	client.EditC.AddEdge(context.Background(), &GraphElement{Graph: graph, Edge: &e})
	return nil
}

// AddSubGraph adds a complete subgraph to an existing graph
func (client Client) AddSubGraph(graph string, g Graph) error {
	client.EditC.AddSubGraph(context.Background(), &Graph{Graph: graph, Edges: g.Edges, Vertices: g.Vertices})
	return nil
}

// StreamElements allows for bulk continuous loading of graph elements into the datastore
func (client Client) StreamElements(elemChan chan GraphElement) error {
	sc, err := client.EditC.StreamElements(context.Background())
	if err != nil {
		return err
	}
	for elem := range elemChan {
		err := sc.Send(&elem)
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
