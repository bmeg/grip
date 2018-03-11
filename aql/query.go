package aql

import (
	"fmt"
	"github.com/bmeg/arachne/protoutil"
	"strings"
)

// V starts a new vertex query, short for `NewQuery().V()`.
func V(ids ...string) *Query {
	return NewQuery().V(ids...)
}

// E starts a new vertex query, short for `NewQuery().E()`.
func E(ids ...string) *Query {
	return NewQuery().E(ids...)
}

// NewQuery creates a new Query instance.
func NewQuery() *Query {
	return &Query{}
}

// Query helps build graph queries.
type Query struct {
	Statements []*GraphStatement
}

func (q *Query) with(st *GraphStatement) *Query {
	nq := &Query{
		Statements: make([]*GraphStatement, len(q.Statements)),
	}
	copy(nq.Statements, q.Statements)
	nq.Statements = append(nq.Statements, st)
	return nq
}

// V adds a vertex selection step to the query
func (q *Query) V(id ...string) *Query {
	vlist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_V{vlist}})
}

// E adds a edge selection step to the query
func (q *Query) E(ids ...string) *Query {
	if len(ids) > 0 {
		return q.with(&GraphStatement{&GraphStatement_E{ids[0]}})
	}
	return q.with(&GraphStatement{&GraphStatement_E{}})
}

// In follows incoming edges to adjacent vertex
func (q *Query) In(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_In{vlist}})
}

// InEdge moves to incoming edge
func (q *Query) InEdge(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_InEdge{vlist}})
}

// Out follows outgoing edges to adjacent vertex
func (q *Query) Out(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_Out{vlist}})
}

// Both follows both incoming and outgoing edges to adjacent vertex
func (q *Query) Both(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_Both{vlist}})
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

// Has filters elements based on data properties.
func (q *Query) Has(key string, value ...string) *Query {
	return q.with(&GraphStatement{&GraphStatement_Has{
		&HasStatement{key, value}}})
}

// HasID filters elements based on element ID.
func (q *Query) HasID(id ...string) *Query {
	idList := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_HasId{idList}})
}

// Limit limits the number of results returned.
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

// Values changes the result to be values from the element data at the given key.
func (q *Query) Values(keys ...string) *Query {
	idList := SelectStatement{keys}
	return q.with(&GraphStatement{&GraphStatement_Values{&idList}})
}

// Match is used to concatenate multiple queries.
func (q *Query) Match(qs ...*Query) *Query {
	queries := []*GraphQuery{}
	for _, q := range qs {
		queries = append(queries, &GraphQuery{
			Query: q.Statements,
		})
	}
	set := &GraphQuerySet{queries}
	return q.with(&GraphStatement{&GraphStatement_Match{set}})
}

// Count adds a count step to the query
func (q *Query) Count() *Query {
	return q.with(&GraphStatement{&GraphStatement_Count{}})
}

func (q *Query) String() string {
	parts := []string{}
	add := func(name string, x ...string) {
		args := strings.Join(x, ", ")
		parts = append(parts, fmt.Sprintf("%s(%s)", name, args))
	}

	for _, gs := range q.Statements {
		switch stmt := gs.GetStatement().(type) {
		case *GraphStatement_V:
			ids := protoutil.AsStringList(stmt.V)
			add("V", ids...)

		case *GraphStatement_E:
			add("E", stmt.E)

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
			add("Values", stmt.Values.Labels...)

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
