package gripql

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

// Index starts a new vertex query using an index hits to start
func Index(field, term string) *Query {
	return NewQuery().Index(field,term)
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
	return q.with(&GraphStatement{Statement: &GraphStatement_V{vlist}})
}

// E adds a edge selection step to the query
func (q *Query) E(id ...string) *Query {
	elist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{Statement: &GraphStatement_E{elist}})
}

// Index adds a index selection step to the query
func (q *Query) Index(field, term string) *Query {
	return q.with(&GraphStatement{
		Statement: &GraphStatement_Index{ &IndexQuery{Field:field, Value:term},
	}})
}

// In follows incoming edges to adjacent vertex
func (q *Query) In(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_In{vlist}})
}

// InV follows incoming edges to adjacent vertex
func (q *Query) InV(label ...string) *Query {
	return q.In(label...)
}

// InE moves to incoming edge
func (q *Query) InE(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_InE{vlist}})
}

// Out follows outgoing edges to adjacent vertex
func (q *Query) Out(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_Out{vlist}})
}

// OutV follows outgoing edges to adjacent vertex
func (q *Query) OutV(label ...string) *Query {
	return q.Out(label...)
}

// OutE moves to outgoing edge
func (q *Query) OutE(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_OutE{vlist}})
}

// Both follows both incoming and outgoing edges to adjacent vertex
func (q *Query) Both(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_Both{vlist}})
}

// BothV follows both incoming and outgoing edges to adjacent vertex
func (q *Query) BothV(label ...string) *Query {
	return q.Both(label...)
}

// BothE moves to both incoming and outgoing edges
func (q *Query) BothE(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_BothE{vlist}})
}

// Has filters elements based on data properties.
func (q *Query) Has(expression *HasExpression) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Has{expression}})
}

// HasLabel filters elements based on their label.
func (q *Query) HasLabel(label ...string) *Query {
	vlist := protoutil.AsListValue(label)
	return q.with(&GraphStatement{Statement: &GraphStatement_HasLabel{vlist}})
}

// HasKey filters elements based on whether it has one or more properties.
func (q *Query) HasKey(key ...string) *Query {
	vlist := protoutil.AsListValue(key)
	return q.with(&GraphStatement{Statement: &GraphStatement_HasKey{vlist}})
}

// HasID filters elements based on their id.
func (q *Query) HasID(id ...string) *Query {
	vlist := protoutil.AsListValue(id)
	return q.with(&GraphStatement{Statement: &GraphStatement_HasId{vlist}})
}

// Limit limits the number of results returned.
func (q *Query) Limit(n uint32) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Limit{n}})
}

// Skip will drop the first n number of records and return the rest.
func (q *Query) Skip(n uint32) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Skip{n}})
}

// Range will limits which records are returned. When the low-end of the range is
// not met, objects are continued to be iterated. When within the low (inclusive)
// and high (exclusive) range, traversers are emitted. When above the high range,
// the traversal breaks out of iteration. Finally, the use of -1 on the high range
// will emit remaining traversers after the low range begins.
func (q *Query) Range(start, stop int32) *Query {
	return q.with(&GraphStatement{
		Statement: &GraphStatement_Range{
			&Range{
				Start: start,
				Stop:  stop,
			},
		},
	})
}

// As marks current elements with tag
func (q *Query) As(id string) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_As{id}})
}

// Select retreieves previously marked elemets
func (q *Query) Select(id ...string) *Query {
	idList := SelectStatement{Marks: id}
	return q.with(&GraphStatement{Statement: &GraphStatement_Select{&idList}})
}

// Fields selects which properties are returned in the result.
func (q *Query) Fields(keys ...string) *Query {
	klist := protoutil.AsListValue(keys)
	return q.with(&GraphStatement{Statement: &GraphStatement_Fields{klist}})
}

// Match is used to concatenate multiple queries.
func (q *Query) Match(qs ...*Query) *Query {
	queries := []*QuerySet{}
	for _, q := range qs {
		queries = append(queries, &QuerySet{
			Query: q.Statements,
		})
	}
	set := &MatchQuerySet{Queries: queries}
	return q.with(&GraphStatement{Statement: &GraphStatement_Match{set}})
}

// Count adds a count step to the query
func (q *Query) Count() *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Count{}})
}

// Distinct selects records with distinct elements of arg
func (q *Query) Distinct(args ...string) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Distinct{protoutil.AsListValue(args)}})
}

// Render adds a render step to the query
func (q *Query) Render(template interface{}) *Query {
	return q.with(&GraphStatement{Statement: &GraphStatement_Render{protoutil.WrapValue(template)}})
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

		case *GraphStatement_InE:
			ids := protoutil.AsStringList(stmt.InE)
			add("InE", ids...)

		case *GraphStatement_OutE:
			ids := protoutil.AsStringList(stmt.OutE)
			add("OutE", ids...)

		case *GraphStatement_BothE:
			ids := protoutil.AsStringList(stmt.BothE)
			add("BothE", ids...)

		case *GraphStatement_Has:
			add("Has")

		case *GraphStatement_HasLabel:
			labels := protoutil.AsStringList(stmt.HasLabel)
			add("HasLabel", labels...)

		case *GraphStatement_HasId:
			ids := protoutil.AsStringList(stmt.HasId)
			add("HasId", ids...)

		case *GraphStatement_HasKey:
			keys := protoutil.AsStringList(stmt.HasKey)
			add("HasKey", keys...)

		case *GraphStatement_Limit:
			add("Limit", fmt.Sprintf("%d", stmt.Limit))

		case *GraphStatement_Skip:
			add("Skip", fmt.Sprintf("%d", stmt.Skip))

		case *GraphStatement_Range:
			add("Range", fmt.Sprintf("%d", stmt.Range.Start), fmt.Sprintf("%d", stmt.Range.Stop))

		case *GraphStatement_Count:
			add("Count")

		case *GraphStatement_As:
			add("As", stmt.As)

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
