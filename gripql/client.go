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
	JobC       JobClient
	ConfigureC ConfigureClient
	conn       *grpc.ClientConn
}

// WrapClient takes previously initialized GRPC clients and uses them for the
// client wrapper
func WrapClient(QueryC QueryClient, EditC EditClient, JobC JobClient, ConfigureC ConfigureClient) Client {
	return Client{QueryC, EditC, JobC, ConfigureC, nil}
}

// Connect opens a GRPC connection to an Grip server
func Connect(conf rpc.Config, write bool) (Client, error) {
	conn, err := rpc.Dial(context.Background(), conf)
	if err != nil {
		return Client{}, err
	}
	queryOut := NewQueryClient(conn)
	var editOut EditClient
	var jobOut JobClient
	if write {
		editOut = NewEditClient(conn)
		jobOut = NewJobClient(conn)

	}
	return Client{queryOut, editOut, jobOut, nil, conn}, nil
}

func (client Client) WithConfigureAPI() Client {
	return Client{client.QueryC, client.EditC, nil, NewConfigureClient(client.conn), client.conn}
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
func (client Client) SampleSchema(graph string) (*Graph, error) {
	out, err := client.EditC.SampleSchema(context.Background(), &GraphID{Graph: graph})
	return out, err
}

// AddSchema adds a schema for a graph.
func (client Client) AddSchema(graph *Graph) error {
	_, err := client.EditC.AddSchema(context.Background(), graph)
	return err
}

func (client Client) DeleteEdge(graph string, id string) error {
	_, err := client.EditC.DeleteEdge(context.Background(), &ElementID{Graph: graph, Id: id})
	return err
}

func (client Client) DeleteVertex(graph string, id string) error {
	_, err := client.EditC.DeleteVertex(context.Background(), &ElementID{Graph: graph, Id: id})
	return err
}

func (client Client) GetEdge(graph string, id string) (*Edge, error) {
	e, err := client.QueryC.GetEdge(context.Background(), &ElementID{Graph: graph, Id: id})
	return e, err
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
func (client Client) Traversal(ctx context.Context, query *GraphQuery) (chan *QueryResult, error) {
	out := make(chan *QueryResult, 100)
	tclient, err := client.QueryC.Traversal(ctx, query)
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

func (client Client) ListJobs(graph string) ([]*QueryJob, error) {
	out := []*QueryJob{}
	tclient, err := client.JobC.ListJobs(context.Background(), &GraphID{Graph: graph})
	if err != nil {
		return nil, err
	}
	for {
		t, err := tclient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

/*
func (client Client) SearchJobs(in *GraphQuery, opts ...grpc.CallOption) (Job_SearchJobsClient, error) {

}
*/

func (client Client) Submit(query *GraphQuery) (*QueryJob, error) {
	return client.JobC.Submit(context.Background(), query)
}

func (client Client) DeleteJob(graph string, jobID string) (*JobStatus, error) {
	return client.JobC.DeleteJob(context.Background(), &QueryJob{Graph: graph, Id: jobID})
}

func (client Client) GetJob(graph string, jobID string) (*JobStatus, error) {
	return client.JobC.GetJob(context.Background(), &QueryJob{Graph: graph, Id: jobID})
}

func (client Client) ResumeJob(graph string, jobID string, q *GraphQuery) (chan *QueryResult, error) {
	tclient, err := client.JobC.ResumeJob(context.Background(),
		&ExtendQuery{Graph: graph,
			SrcId: jobID,
			Query: q.Query,
		})

	out := make(chan *QueryResult, 100)

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

func (client Client) ViewJob(graph string, jobID string, opts ...grpc.CallOption) (chan *QueryResult, error) {
	tclient, err := client.JobC.ViewJob(context.Background(),
		&QueryJob{Graph: graph,
			Id: jobID,
		})

	out := make(chan *QueryResult, 100)

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
