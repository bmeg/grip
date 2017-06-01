
package aql

import (
	"log"
	"fmt"
	"context"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/jsonpb"
	"encoding/json"
)


func Connect(address string) (QueryClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
  if err != nil {
    return nil, err
  }
  out := NewQueryClient(conn)
  return out, err
}

type QueryBuilder struct {
	client QueryClient
	query  []*GraphStatement
}

func Query(client QueryClient) QueryBuilder {
	return QueryBuilder{client, []*GraphStatement{}}
}

func (q QueryBuilder) V(id ...string) QueryBuilder {
	if len(id) > 0 {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_V{id[0]}}) }
	} else {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_V{}}) }
	}
}

func (q QueryBuilder) AddV(id string) QueryBuilder {
	return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_AddV{id}}) }
}

func (q QueryBuilder) AddE(edgeType string) QueryBuilder {
	return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_AddE{edgeType}}) }
}


func (q QueryBuilder) To(id string) QueryBuilder {
	return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_To{id}}) }
}

func (q QueryBuilder) Property(v ...interface{}) QueryBuilder {
	if len(v) == 1 {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{
			&GraphStatement_Property{ AsStruct(v[0].(map[string]interface{})) },
		})}
	} else {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{
			&GraphStatement_Property{ AsStruct( map[string]interface{}{
				v[0].(string) : v[1],
			} ) },
		})}
	}
}

func (q QueryBuilder) Count() QueryBuilder {
	return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_Count{}}) }
}


func (q QueryBuilder) Execute() (chan *ResultRow, error) {
	tclient, err := q.client.Traversal(context.TODO(), &GraphQuery{q.query})
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
	s, _ := m.MarshalToString(&GraphQuery{q.query})
	fmt.Printf("%s = %s\n", q, s)
	out := map[string]interface{}{}
	json.Unmarshal([]byte(s), &out)
	return out
}
