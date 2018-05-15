package aql

import (
	"context"
	"io"
	"log"

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
		return Client{queryOut, nil}, nil
	}
	editOut := NewEditClient(conn)
	return Client{queryOut, editOut}, nil
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
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println("Failed to list graphs:", err)
				return
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
	ts, err := client.QueryC.GetTimestamp(context.Background(), &GraphID{Graph: graph})
	return ts, err
}

// DeleteGraph deletes a graph and all of its contents
func (client Client) DeleteGraph(graph string) error {
	_, err := client.EditC.DeleteGraph(context.Background(), &GraphID{Graph: graph})
	return err
}

// AddGraph creates a new graph
func (client Client) AddGraph(graph string) error {
	_, err := client.EditC.AddGraph(context.Background(), &GraphID{Graph: graph})
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

// BulkAdd allows for bulk continuous loading of graph elements into the datastore
func (client Client) BulkAdd(elemChan chan *GraphElement) error {
	sc, err := client.EditC.BulkAdd(context.Background())
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

// Traversal runs a graph traversal query
func (client Client) Traversal(query *GraphQuery) (chan *QueryResult, error) {
	tclient, err := client.QueryC.Traversal(context.Background(), query)
	if err != nil {
		return nil, err
	}

	out := make(chan *QueryResult, 100)
	go func() {
		defer close(out)
		for {
			t, err := tclient.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println("Failed to receive traversal result:", err)
				return
			}
			out <- t
		}
	}()

	return out, nil
}
