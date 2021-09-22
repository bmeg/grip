package gripql

import (
	"context"
	"io"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"google.golang.org/grpc"
)

// Client is a GRPC grip client with some helper functions
type Client struct {
	QueryC     QueryClient
	EditC      EditClient
	ConfigureC ConfigureClient
	conn       *grpc.ClientConn
}

// WrapClient takes previously initialized GRPC clients and uses them for the
// client wrapper
func WrapClient(QueryC QueryClient, EditC EditClient, ConfigureC ConfigureClient) Client {
	return Client{QueryC, EditC, ConfigureC, nil}
}

// Connect opens a GRPC connection to an Grip server
func Connect(conf rpc.Config, write bool) (Client, error) {
	conn, err := rpc.Dial(context.Background(), conf)
	if err != nil {
		return Client{}, err
	}
	queryOut := NewQueryClient(conn)
	var editOut EditClient
	if write {
		editOut = NewEditClient(conn)
	}
	return Client{queryOut, editOut, nil, conn}, nil
}

func (client Client) WithConfigureAPI() Client {
	return Client{client.QueryC, client.EditC, NewConfigureClient(client.conn), client.conn}
}

// Close the connection
func (client Client) Close() {
	client.conn.Close()
}

// GetSchema returns the schema for the given graph.
func (client Client) GetSchema(graph string) (*Graph, error) {
	return client.QueryC.GetSchema(context.Background(), &GraphID{Graph: graph})
}

// SampleSchema asks server to scan data to determine possible schema
func (client Client) SampleSchema(graph string) (*Graph, error)  {
	out, err := client.EditC.SampleSchema(context.Background(), &GraphID{Graph: graph})
	return out, err
}


// AddSchema adds a schema for a graph.
func (client Client) AddSchema(graph *Graph) error {
	_, err := client.EditC.AddSchema(context.Background(), graph)
	return err
}

// GetMaping returns the mapping for the given graph.
func (client Client) GetMapping(graph string) (*Graph, error) {
	return client.QueryC.GetMapping(context.Background(), &GraphID{Graph: graph})
}

// AddMapping adds a mapping for a graph.
func (client Client) AddMapping(graph *Graph) error {
	_, err := client.EditC.AddMapping(context.Background(), graph)
	return err
}

// ListGraphs lists the graphs in the database
func (client Client) ListGraphs() (*ListGraphsResponse, error) {
	return client.QueryC.ListGraphs(context.Background(), &Empty{})
}

// ListTables lists the tables in the database
func (client Client) ListTables() (chan *TableInfo, error) {
	out := make(chan *TableInfo, 10)

	clt, err := client.QueryC.ListTables(context.Background(), &Empty{})
	if err != nil {
		close(out)
		return out, err
	}
	go func() {
		defer close(out)
		for {
			t, err := clt.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Receiving table list")
				return
			}
			out <- t
		}
	}()
	return out, nil
}

// ListIndices lists the indices on a graph in the database
func (client Client) ListIndices(graph string) (*ListIndicesResponse, error) {
	return client.QueryC.ListIndices(context.Background(), &GraphID{Graph: graph})
}

// ListLabels lists the vertex and edge labels for a graph in the database
func (client Client) ListLabels(graph string) (*ListLabelsResponse, error) {
	return client.QueryC.ListLabels(context.Background(), &GraphID{Graph: graph})
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
				log.WithFields(log.Fields{"error": err}).Error("Receiving traversal result")
				return
			}
			out <- t
		}
	}()

	return out, nil
}

// ListDrivers lists avalible drivers
func (client Client) ListDrivers() (*ListDriversResponse, error) {
	return client.ConfigureC.ListDrivers(context.Background(), &Empty{})
}

// ListPlugins
func (client Client) ListPlugins() (*ListPluginsResponse, error) {
	return client.ConfigureC.ListPlugins(context.Background(), &Empty{})
}

// ListPlugins
func (client Client) StartPlugin(conf *PluginConfig) (*PluginStatus, error) {
	return client.ConfigureC.StartPlugin(context.Background(), conf)
}
