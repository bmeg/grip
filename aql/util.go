
package aql

import (
	"log"
	"fmt"
	"context"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"encoding/json"
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

func (client AQLClient) AddV(graph string, v Vertex) error {
	client.EditC.AddVertex(context.Background(), &GraphElement{Graph:graph,Vertex:&v})
	return nil
}

func (client AQLClient) AddE(graph string, e Edge) error {
	client.EditC.AddEdge(context.Background(), &GraphElement{Graph:graph, Edge:&e})
	return nil
}

func (client AQLClient) Query(graph string) QueryBuilder {
	return QueryBuilder{client.QueryC, graph, []*GraphStatement{}}
}

func (q QueryBuilder) V(id ...string) QueryBuilder {
	if len(id) > 0 {
		return QueryBuilder{ q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_V{id[0]}}) }
	} else {
		return QueryBuilder{ q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_V{}}) }
	}
}


func (q QueryBuilder) Count() QueryBuilder {
	return QueryBuilder{ q.client, q.graph, append(q.query, &GraphStatement{&GraphStatement_Count{}}) }
}


func (q QueryBuilder) Execute() (chan *ResultRow, error) {
	tclient, err := q.client.Traversal(context.TODO(), &GraphQuery{q.graph, q.query})
	if err != nil {
		return nil, err
	}
	out := make(chan *ResultRow, 100)
	go func() {
		defer close(out)
		for t, err := tclient.Recv(); err != nil; t, err = tclient.Recv() {
			log.Printf("vert: %s\n", t)
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
	for range c {}
	return nil
}

func (q QueryBuilder) Render() map[string]interface{} {
	m := jsonpb.Marshaler{}
	s, _ := m.MarshalToString(&GraphQuery{q.graph, q.query})
	fmt.Printf("%s = %s\n", q, s)
	out := map[string]interface{}{}
	json.Unmarshal([]byte(s), &out)
	return out
}


func (vertex *Vertex) SetProperty(key string, value interface{}) {
	if vertex.Properties == nil {
		vertex.Properties = &structpb.Struct{Fields:map[string]*structpb.Value{} }
	}
	//BUG: This is only supporting strings at the moment
	vertex.Properties.Fields[key] = &structpb.Value{Kind: &structpb.Value_StringValue{value.(string)}}
}
