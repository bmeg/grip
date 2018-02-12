package aql

import (
	"bytes"
	"encoding/json"
  "fmt"
  "strings"
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
  nq.GraphQuery.Query = make([]*GraphStatement, len(q.GraphQuery.Query))
	copy(nq.GraphQuery.Query, q.GraphQuery.Query)
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
	vlist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_E{vlist}})
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

func (q *Query) Has(key string, value ...string) *Query {
  return q.with(&GraphStatement{&GraphStatement_Has{
    &HasStatement{key, value}}})
}

func (q *Query) HasID(id ...string) *Query {
	idList := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_HasId{idList}})
}

func (q *Query) Limit(c int64) *Query {
  return q.with(&GraphStatement{&GraphStatement_Limit{c}})
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

func (q *Query) String() string {
  parts := []string{}
  add := func(name string, x ...string) {
    args := strings.Join(x, ", ")
    parts = append(parts, fmt.Sprintf("%s(%s)", name, args))
  }

  for _, gs := range q.GraphQuery.Query {
    switch stmt := gs.Statement.(type) {
    case *GraphStatement_V:
      ids := protoutil.AsStringList(stmt.V)
      add("V", ids...)

    case *GraphStatement_E:
      ids := protoutil.AsStringList(stmt.E)
      add("E", ids...)

    case *GraphStatement_Has:
      args := []string{stmt.Has.Key}
      args = append(args, stmt.Has.Within...)
      add("Has", args...)

    case *GraphStatement_HasLabel:
      ids := protoutil.AsStringList(stmt.HasLabel)
      add("HasLabel", ids...)

    case *GraphStatement_HasId:
      ids := protoutil.AsStringList(stmt.HasId)
      add("HasId", ids...)

    case *GraphStatement_In:
      ids := protoutil.AsStringList(stmt.In)
      add("In", ids...)

    case *GraphStatement_Out:
      ids := protoutil.AsStringList(stmt.Out)
      add("Out", ids...)

    case *GraphStatement_Both:
      ids := protoutil.AsStringList(stmt.Both)
      add("Both", ids...)

    case *GraphStatement_InEdge:
      ids := protoutil.AsStringList(stmt.InEdge)
      add("InEdge", ids...)

    case *GraphStatement_OutEdge:
      ids := protoutil.AsStringList(stmt.OutEdge)
      add("OutEdge", ids...)

    case *GraphStatement_BothEdge:
      ids := protoutil.AsStringList(stmt.BothEdge)
      add("BothEdge", ids...)

    case *GraphStatement_Limit:
      add("Limit", fmt.Sprintf("%d", stmt.Limit))

    case *GraphStatement_Count:
      add("Count")

    case *GraphStatement_GroupCount:
      add("GroupCount", stmt.GroupCount)

    case *GraphStatement_As:
      add("As", stmt.As)

    case *GraphStatement_Select:
      add("Select", stmt.Select.Labels...)
    case *GraphStatement_Match:
      add("Match")
    case *GraphStatement_Values:
      add("Values")

    case *GraphStatement_Import:
      add("Import")
    case *GraphStatement_Map:
      add("Map")
    case *GraphStatement_Fold:
      add("Fold")
    case *GraphStatement_Filter:
      add("Filter")
    case *GraphStatement_FilterValues:
      add("FilterValues")
    case *GraphStatement_VertexFromValues:
      add("VertexFromValues")
    }
  }
  return strings.Join(parts, ".")
}
