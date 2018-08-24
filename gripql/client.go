package gripql

import (
	"context"
	"io"
	"log"

	"github.com/bmeg/grip/util/rpc"
)

// Client is a GRPC grip client with some helper functions
type Client struct {
	QueryC QueryClient
	EditC  EditClient
}

// Connect opens a GRPC connection to an Grip server
func Connect(conf rpc.Config, write bool) (Client, error) {
	conn, err := rpc.Dial(context.Background(), conf)
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

// GetSchema returns the schema for the given graph.
func (client Client) GetSchema(graph string) (*GraphSchema, error) {
	return client.QueryC.GetSchema(context.Background(), &GraphID{Graph: graph})
}

// ListGraphs lists the graphs in the database
func (client Client) ListGraphs() (chan string, error) {
	out := make(chan string, 100)
	cl, err := client.QueryC.ListGraphs(context.Background(), &Empty{})
	if err != nil {
		return nil, err
	}

	elem, err := cl.Recv()
	if err == io.EOF {
		close(out)
		return out, nil
	}
	if err != nil {
		close(out)
		return out, err
	}
	out <- elem.Graph

	go func() {
		defer close(out)
		for {
			elem, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println("Error: listing graphs:", err)
				return
			}
			out <- elem.Graph
		}
	}()

	return out, nil
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
	out := make(chan *QueryResult, 100)
	tclient, err := client.QueryC.Traversal(context.Background(), query)
	if err != nil {
		return nil, err
	}

	t, err := tclient.Recv()
	if err == io.EOF {
		close(out)
		return out, nil
	}
	if err != nil {
		close(out)
		return out, err
	}
	out <- t

	go func() {
		defer close(out)
		for {
			t, err := tclient.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println("Error: receiving traversal result:", err)
				return
			}
			out <- t
		}
	}()

	return out, nil
}
