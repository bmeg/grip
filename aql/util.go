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

type AQLClient struct {
	QueryC QueryClient
	EditC  EditClient
}

func Connect(address string, write bool) (AQLClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return AQLClient{}, err
	}
	query_out := NewQueryClient(conn)
	if !write {
		return AQLClient{query_out, nil}, err
	} else {
		edit_out := NewEditClient(conn)
		return AQLClient{query_out, edit_out}, err
	}
}

type QueryBuilder struct {
	client QueryClient
	graph  string
	query  []*GraphStatement
}

func (client AQLClient) GetGraphs() chan string {
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

func (client AQLClient) DeleteGraph(graph string) error {
	_, err := client.EditC.DeleteGraph(context.Background(), &ElementID{Graph: graph})
	return err
}

func (client AQLClient) AddGraph(graph string) error {
	_, err := client.EditC.AddGraph(context.Background(), &ElementID{Graph: graph})
	return err
}

func (client AQLClient) AddVertex(graph string, v Vertex) error {
	client.EditC.AddVertex(context.Background(), &GraphElement{Graph: graph, Vertex: &v})
	return nil
}

func (client AQLClient) AddEdge(graph string, e Edge) error {
	client.EditC.AddEdge(context.Background(), &GraphElement{Graph: graph, Edge: &e})
	return nil
}

func (client AQLClient) AddBundle(graph string, e Bundle) error {
	client.EditC.AddBundle(context.Background(), &GraphElement{Graph: graph, Bundle: &e})
	return nil
}

func (client AQLClient) StreamElements(elemChan chan GraphElement) error {
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

func (client AQLClient) GetVertex(graph string, id string) (*Vertex, error) {
	v, err := client.QueryC.GetVertex(context.Background(), &ElementID{Graph:graph, Id:id})
	return v, err
}


func (client AQLClient) Query(graph string) QueryBuilder {
	return QueryBuilder{client.QueryC, graph, []*GraphStatement{}}
}

func (q QueryBuilder) V(id ...string) QueryBuilder {
	vlist := protoutil.AsListValue(id)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_V{vlist}})}
}

func (q QueryBuilder) E(id ...string) QueryBuilder {
	if len(id) > 0 {
		return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_E{id[0]}})}
	} else {
		return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_E{}})}
	}
}

func (q QueryBuilder) Out(label ...string) QueryBuilder {
	vlist := protoutil.AsListValue(label)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Out{vlist}})}
}

func (q QueryBuilder) OutEdge(label ...string) QueryBuilder {
	vlist := protoutil.AsListValue(label)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_OutEdge{vlist}})}
}

func (q QueryBuilder) HasLabel(id ...string) QueryBuilder {
	idList := protoutil.AsListValue(id)
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_HasLabel{idList}})}
}

func (q QueryBuilder) As(id string) QueryBuilder {
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_As{id}})}
}

func (q QueryBuilder) Select(id ...string) QueryBuilder {
	idList := SelectStatement{id}
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Select{&idList}})}
}

func (q QueryBuilder) Count() QueryBuilder {
	return QueryBuilder{q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Count{}})}
}

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

func (q QueryBuilder) Run() error {
	c, err := q.Execute()
	if err != nil {
		return err
	}
	for range c {
	}
	return nil
}

func (q QueryBuilder) Render() map[string]interface{} {
	m := jsonpb.Marshaler{}
	s, _ := m.MarshalToString(&GraphQuery{q.graph, q.query})
	//fmt.Printf("%s = %s\n", q, s)
	out := map[string]interface{}{}
	json.Unmarshal([]byte(s), &out)
	return out
}

func (vertex *Vertex) GetDataMap() map[string]interface{} {
	return protoutil.AsMap(vertex.Data)
}


func (vertex *Vertex) SetProperty(key string, value interface{}) {
	if vertex.Data == nil {
		vertex.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	protoutil.StructSet(vertex.Data, key, value)
}

func (vertex *Vertex) GetProperty(key string) interface{} {
	if vertex.Data == nil {
		return nil
	}
	m := protoutil.AsMap(vertex.Data)
	return m[key]
}


func (edge *Edge) GetProperty(key string) interface{} {
	if edge.Data == nil {
		return nil
	}
	m := protoutil.AsMap(edge.Data)
	return m[key]
}
