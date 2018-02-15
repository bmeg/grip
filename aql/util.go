package aql

import (
	"io"
	//"log"
	//"fmt"
	"context"
	"encoding/json"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/jsonpb"
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

// QueryBuilder allows the user to build complex graph queries then serialize
// them and then execute via GRPC
type QueryBuilder struct {
	client QueryClient
	graph  string
	query  []*GraphStatement
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

// AddBundle adds a edge bundle to the graph
func (client Client) AddBundle(graph string, e Bundle) error {
	client.EditC.AddBundle(context.Background(), &GraphElement{Graph: graph, Bundle: &e})
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

// Query initializes a query build for `graph`
func (client Client) Query(graph string) QueryBuilder {
	return QueryBuilder{client.QueryC, graph, []*GraphStatement{}}
}

// V adds a vertex selection step to the query
func (q QueryBuilder) V(id ...string) QueryBuilder {
	vlist := protoutil.AsListValue(id)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_V{vlist}})}
}

// E adds a edge selection step to the query
func (q QueryBuilder) E(id ...string) QueryBuilder {
	if len(id) > 0 {
		return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_E{id[0]}})}
	}
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_E{}})}
}

// Out follows outgoing edges to adjacent vertex
func (q QueryBuilder) Out(label ...string) QueryBuilder {
	vlist := protoutil.AsListValue(label)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Out{vlist}})}
}

// OutEdge moves to outgoing edge
func (q QueryBuilder) OutEdge(label ...string) QueryBuilder {
	vlist := protoutil.AsListValue(label)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_OutEdge{vlist}})}
}

// HasLabel filters elements based on label
func (q QueryBuilder) HasLabel(id ...string) QueryBuilder {
	idList := protoutil.AsListValue(id)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_HasLabel{idList}})}
}

// As marks current elements with tag
func (q QueryBuilder) As(id string) QueryBuilder {
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_As{id}})}
}

// Select retreieves previously marked elemets
func (q QueryBuilder) Select(id ...string) QueryBuilder {
	idList := SelectStatement{id}
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Select{&idList}})}
}

// Count adds a count step to the query
func (q QueryBuilder) Count() QueryBuilder {
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Count{}})}
}

// Execute takes the current query, and makes RPC call then streams the results
func (q QueryBuilder) Execute() (chan *ResultRow, error) {
	tclient, err := q.client.Traversal(context.TODO(), &GraphQuery{q.graph, q.query})
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

// Run takes current query and executes it, ignoring the results
func (q QueryBuilder) Run() error {
	c, err := q.Execute()
	if err != nil {
		return err
	}
	for range c {
	}
	return nil
}

// Render takes the current query build and renders it to a map
func (q QueryBuilder) Render() map[string]interface{} {
	m := jsonpb.Marshaler{}
	s, _ := m.MarshalToString(&GraphQuery{q.graph, q.query})
	//fmt.Printf("%s = %s\n", q, s)
	out := map[string]interface{}{}
	json.Unmarshal([]byte(s), &out)
	return out
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
