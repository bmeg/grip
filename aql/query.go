package aql

import (
	"bytes"
	"encoding/json"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/jsonpb"
)

// Query helps build graph queries.
type Query struct {
	*GraphQuery
}

// NewQuery returns a new query object.
func NewQuery(graph string) *Query {
	return &Query{
		&GraphQuery{
			Graph: graph,
		},
	}
}

func (q *Query) with(st *GraphStatement) *Query {
	nq := NewQuery(q.GraphQuery.Graph)
	copy(q.GraphQuery.Query, nq.GraphQuery.Query)
	nq.GraphQuery.Query = append(nq.GraphQuery.Query, st)
	return nq
}

// V adds a vertex selection step to the query
func (q *Query) V(id ...string) *Query {
	vlist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_V{vlist}})
}

// E adds a edge selection step to the query
func (q *Query) E(id ...string) *Query {
	if len(id) > 0 {
		return q.with(&GraphStatement{&GraphStatement_E{id[0]}})
	}
	return q.with(&GraphStatement{&GraphStatement_E{}})
}

// Out follows outgoing edges to adjacent vertex
func (q *Query) Out(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_Out{vlist}})
}

// OutEdge moves to outgoing edge
func (q *Query) OutEdge(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_OutEdge{vlist}})
}

// HasLabel filters elements based on label
func (q *Query) HasLabel(id ...string) *Query {
	idList := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_HasLabel{idList}})
}

// As marks current elements with tag
func (q *Query) As(id string) *Query {
	return q.with(&GraphStatement{&GraphStatement_As{id}})
}

// Select retreieves previously marked elemets
func (q *Query) Select(id ...string) *Query {
	idList := SelectStatement{id}
	return q.with(&GraphStatement{&GraphStatement_Select{&idList}})
}

// Count adds a count step to the query
func (q *Query) Count() *Query {
	return q.with(&GraphStatement{&GraphStatement_Count{}})
}

// Render renders the to a map.
func (q *Query) Render() map[string]interface{} {
	m := jsonpb.Marshaler{}
	b := &bytes.Buffer{}
	m.Marshal(b, q.GraphQuery)
	out := map[string]interface{}{}
	json.Unmarshal(b.Bytes(), &out)
	return out
}
