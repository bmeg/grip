package aql

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/protoutil"
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
func (q *Query) E(id ...string) *Query {
	elist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{&GraphStatement_E{elist}})
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

// OutEdge moves to outgoing edge
func (q *Query) OutEdge(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_OutEdge{vlist}})
}

// Both follows both incoming and outgoing edges to adjacent vertex
func (q *Query) Both(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_Both{vlist}})
}

// BothEdge moves to both incoming and outgoing edges
func (q *Query) BothEdge(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{&GraphStatement_BothEdge{vlist}})
}

// Where filters elements based on data properties.
func (q *Query) Where(expression *WhereExpression) *Query {
	return q.with(&GraphStatement{&GraphStatement_Where{expression}})
}

// Limit limits the number of results returned.
func (q *Query) Limit(n uint32) *Query {
	return q.with(&GraphStatement{&GraphStatement_Limit{n}})
}

// Offset will drop the first n number of records and return the rest.
func (q *Query) Offset(n uint32) *Query {
	return q.with(&GraphStatement{&GraphStatement_Offset{n}})
}

// Mark marks current elements with tag
func (q *Query) Mark(id string) *Query {
	return q.with(&GraphStatement{&GraphStatement_Mark{id}})
}

// Select retreieves previously marked elemets
func (q *Query) Select(id ...string) *Query {
	idList := SelectStatement{id}
	return q.with(&GraphStatement{&GraphStatement_Select{&idList}})
}

// Fields selects which properties are returned in the result.
func (q *Query) Fields(keys ...string) *Query {
	klist := protoutil.AsListValue(keys)
	return q.with(&GraphStatement{&GraphStatement_Fields{klist}})
}

// Match is used to concatenate multiple queries.
func (q *Query) Match(qs ...*Query) *Query {
	queries := []*QuerySet{}
	for _, q := range qs {
		queries = append(queries, &QuerySet{
			Query: q.Statements,
		})
	}
	set := &MatchQuerySet{queries}
	return q.with(&GraphStatement{&GraphStatement_Match{set}})
}

// Count adds a count step to the query
func (q *Query) Count() *Query {
	return q.with(&GraphStatement{&GraphStatement_Count{}})
}

// Render adds a render step to the query
func (q *Query) Render(template interface{}) *Query {
	return q.with(&GraphStatement{&GraphStatement_Render{protoutil.WrapValue(template)}})
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
			ids := protoutil.AsStringList(stmt.E)
			add("E", ids...)

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

		case *GraphStatement_Where:
			add("Where")

		case *GraphStatement_Limit:
			add("Limit", fmt.Sprintf("%d", stmt.Limit))

		case *GraphStatement_Count:
			add("Count")

		case *GraphStatement_Mark:
			add("Mark", stmt.Mark)

		case *GraphStatement_Select:
			add("Select", stmt.Select.Marks...)

		case *GraphStatement_Match:
			add("Match")

		case *GraphStatement_Fields:
			fields := protoutil.AsStringList(stmt.Fields)
			add("Fields", fields...)

		case *GraphStatement_Aggregate:
			add("Aggregate")

		case *GraphStatement_Render:
			add("Render")
		}
	}

	return strings.Join(parts, ".")
}
