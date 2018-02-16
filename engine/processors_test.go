package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/protoutil"
	"github.com/go-test/deep"
	"github.com/kr/pretty"
	"testing"
	"time"
)

// TODO memgraph doesn't correctly support the "load" flag
var db = memgraph.NewMemGraph()
var Q = aql.Query{}

var verts = []*aql.ResultRow{
	vert("v0", "Human", dat{"name": "Alex"}),
	vert("v1", "Human", dat{"name": "Kyle"}),
	vert("v2", "Human", dat{"name": "Ryan"}),
	vert("v3", "Robot", dat{"name": "C-3PO"}),
	vert("v4", "Robot", dat{"name": "R2-D2"}),
	vert("v5", "Robot", dat{"name": "Bender"}),
	vert("v6", "Clone", dat{"name": "Alex"}),
	vert("v7", "Clone", dat{"name": "Kyle"}),
	vert("v8", "Clone", dat{"name": "Ryan"}),
	vert("v9", "Clone", nil),
	vert("v10", "Project", dat{"name": "Funnel"}),
	vert("v11", "Project", dat{"name": "Gaia"}),
}

var edges = []*aql.ResultRow{
	edge("e0", "v0", "v10", "WorksOn", nil),
	edge("e1", "v2", "v11", "WorksOn", nil),
}

var table = []struct {
	query    *aql.Query
	expected []*aql.ResultRow
}{
	{
		Q.V().Has("name", "Kyle", "Alex"),
		pick(verts, 0, 1, 6, 7),
	},
	{
		Q.V().Has("non-existant", "Kyle", "Alex"),
		pick(verts),
	},
	{
		Q.V().HasLabel("Human"),
		pick(verts, 0, 1, 2),
	},
	{
		Q.V().HasLabel("Robot"),
		pick(verts, 3, 4, 5),
	},
	{
		Q.V().HasLabel("Robot", "Human"),
		pick(verts, 0, 1, 2, 3, 4, 5),
	},
	{
		Q.V().HasLabel("non-existant"),
		pick(verts),
	},
	{
		Q.V().HasID("v0", "v2"),
		pick(verts, 0, 2),
	},
	{
		Q.V().HasID("non-existant"),
		pick(verts),
	},
	{
		Q.V().Limit(2),
		pick(verts, 0, 1),
	},
	{
		Q.V().Count(),
		[]*aql.ResultRow{
			{
				Value: &aql.QueryResult{
					&aql.QueryResult_IntValue{IntValue: int32(len(verts))},
				},
			},
		},
	},
	{
		Q.V().HasLabel("Human").Has("name", "Ryan"),
		pick(verts, 2),
	},
	{
		Q.V().HasLabel("Human").
			As("x").Has("name", "Alex").Select("x"),
		pick(verts, 0),
	},
	{
		Q.V(),
		verts,
	},
	{
		Q.E(),
		edges,
	},
	{
		Q.V().HasLabel("Human").Out(),
		pick(verts, 10, 11),
	},
	{
		Q.V().HasLabel("Human").Out().Has("name", "Funnel"),
		pick(verts, 10),
	},
	{
		Q.V().HasLabel("Human").As("x").Out().Has("name", "Funnel").Select("x"),
		pick(verts, 0),
	},
	{
		Q.V().HasLabel("Human").OutEdge(),
		edges,
	},
	{
		Q.V().HasLabel("Human").Has("name", "Alex").OutEdge(),
		pick(edges, 0),
	},
	{
		Q.V().HasLabel("Human").Has("name", "Alex").OutEdge().As("x"),
		pick(edges, 0),
	},
	{
		Q.V().HasLabel("Human").Values(),
		values_("Alex", "Kyle", "Ryan"),
	},
	{
		Q.V().Match(
			Q.HasLabel("Human"),
			Q.Has("name", "Alex"),
		),
		pick(verts, 0),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("c"),
		pick(verts, 0),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("c"),
		pick(verts, 0),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("b", "c"),
		pick(verts, 0),
	},
	/*
	  TODO fairly certain match does not support this query from the gremlin docs
	  gremlin> graph.io(graphml()).readGraph('data/grateful-dead.xml')
	  gremlin> g = graph.traversal()
	  ==>graphtraversalsource[tinkergraph[vertices:808 edges:8049], standard]
	  gremlin> g.V().match(
	                   __.as('a').has('name', 'Garcia'),
	                   __.as('a').in('writtenBy').as('b'),
	                   __.as('a').in('sungBy').as('b')).
	                 select('b').values('name')
	  ==>CREAM PUFF WAR
	  ==>CRYPTICAL ENVELOPMENT
	*/
}

func TestProcs(t *testing.T) {
	for _, desc := range table {
		t.Run(desc.query.String(), func(t *testing.T) {
			// Catch pipes which forget to close their out channel
			// by requiring they process quickly.
			timer := time.NewTimer(time.Millisecond * 100)
			// "done" is closed when the pipe finishes.
			done := make(chan struct{})

			go func() {
				defer close(done)

				ctx := context.Background()
				res, err := Run(ctx, desc.query.Statements, db)
				if err != nil {
					t.Fatal(err)
				}

				if !timer.Stop() {
					<-timer.C
				}
				pretty.Println(res)
				pretty.Println(desc.expected)
				diff := deep.Equal(res, desc.expected)
				pretty.Println(diff)
				if diff != nil {
					t.Error(diff)
				}
			}()

			select {
			case <-done:
			case <-timer.C:
				t.Log("did you forget to close the out channel?")
				t.Fatal("pipe failed to process in time")
			}
		})
	}
}

func pick(src []*aql.ResultRow, is ...int) []*aql.ResultRow {
	out := []*aql.ResultRow{}
	for _, i := range is {
		out = append(out, src[i])
	}
	return out
}

func vert(id, label string, d dat) *aql.ResultRow {
	v := &aql.Vertex{
		Gid:   id,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
	db.AddVertex(v)
	return &aql.ResultRow{
		Value: &aql.QueryResult{
			&aql.QueryResult_Vertex{v},
		},
	}
}

func values_(vals ...interface{}) []*aql.ResultRow {
	out := []*aql.ResultRow{}
	/*
		for _, val := range vals {
			out = append(out, &aql.ResultRow{
			})
		}
	*/
	return out
}

func edge(id, from, to, label string, d dat) *aql.ResultRow {
	v := &aql.Edge{
		Gid:   id,
		From:  from,
		To:    to,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
	db.AddEdge(v)
	return &aql.ResultRow{
		Value: &aql.QueryResult{
			&aql.QueryResult_Edge{v},
		},
	}
}

type dat map[string]interface{}
