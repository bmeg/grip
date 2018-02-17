package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/kvgraph"
	_ "github.com/bmeg/arachne/boltdb"
  "github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/protoutil"
	"github.com/go-test/deep"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/kr/pretty"
  "os"
  "reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

var Q = aql.Query{}

var verts = []interface{}{
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

var edges = []interface{}{
	edge("e0", "v0", "v10", "WorksOn", nil),
	edge("e1", "v2", "v11", "WorksOn", nil),
}

var table = []struct {
	query    *aql.Query
	expected []*aql.ResultRow
}{
	{
		Q.V().Has("name", "Kyle", "Alex"),
		pick(verts[0], verts[1], verts[6], verts[7]),
	},
	{
		Q.V().Has("non-existant", "Kyle", "Alex"),
		pick(),
	},
	{
		Q.V().HasLabel("Human"),
		pick(verts[0:3]...),
	},
	{
		Q.V().HasLabel("Robot"),
		pick(verts[3:6]...),
	},
	{
		Q.V().HasLabel("Robot", "Human"),
		pick(verts[0:6]...),
	},
	{
		Q.V().HasLabel("non-existant"),
		pick(),
	},
	{
		Q.V().HasID("v0", "v2"),
		pick(verts[0], verts[2]),
	},
	{
		Q.V().HasID("non-existant"),
		pick(),
	},
	{
		Q.V().Limit(2),
		pick(verts[0:2]...),
	},
	{
		Q.V().Count(),
		[]*aql.ResultRow{
			{
				Value: &aql.QueryResult{
					&aql.QueryResult_Data{
						&structpb.Value{
							Kind: &structpb.Value_NumberValue{
								// TODO wrong. should be int.
								NumberValue: float64(len(verts)),
							},
						},
					},
				},
			},
		},
	},
	{
		Q.V().HasLabel("Human").Has("name", "Ryan"),
		pick(verts[2]),
	},
	{
		Q.V().HasLabel("Human").
			As("x").Has("name", "Alex").Select("x"),
		pick(verts[0]),
	},
	{
		Q.V(),
		pick(verts...),
	},
	{
		Q.E(),
		pick(edges...),
	},
	{
		Q.V().HasLabel("Human").Out(),
		pick(verts[10], verts[11]),
	},
	{
		Q.V().HasLabel("Human").Out().Has("name", "Funnel"),
		pick(verts[10]),
	},
	{
		Q.V().HasLabel("Human").As("x").Out().Has("name", "Funnel").Select("x"),
		pick(verts[0]),
	},
	{
		Q.V().HasLabel("Human").OutEdge(),
		pick(edges...),
	},
	{
		Q.V().HasLabel("Human").Has("name", "Alex").OutEdge(),
		pick(edges[0]),
	},
	{
		Q.V().HasLabel("Human").Has("name", "Alex").OutEdge().As("x"),
		pick(edges[0]),
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
		pick(verts[0]),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("c"),
		pick(verts[0]),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("c"),
		pick(verts[0]),
	},
	{
		Q.V().Match(
			Q.As("a").HasLabel("Human").As("b"),
			Q.As("b").Has("name", "Alex").As("c"),
		).Select("b", "c"),
		[]*aql.ResultRow{
			{
				Row: pickrow(verts[0], verts[0]),
			},
		},
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

func TestEngine(t *testing.T) {
  defer os.RemoveAll("test-badger.db")
  defer os.Remove("test-bolt.db")

  bolt, err := kvgraph.NewKVArachne("bolt", "test-bolt.db")
  if err != nil {
    t.Fatal(err)
  }

	dbs := map[string]gdbi.GraphDB{
		// TODO memgraph doesn't correctly support the "load" flag
		"mem": memgraph.NewMemGraph(),
    "bolt": bolt.Graph("test-graph"),
    "badger": badgerdb.NewBadgerArachne("test-badger.db").Graph("test-graph"),
	}

	for dbname, db := range dbs {

		for _, v := range verts {
			db.AddVertex(v.(*aql.Vertex))
		}
		for _, e := range edges {
			db.AddEdge(e.(*aql.Edge))
		}

		for _, desc := range table {
			name := cleanName(dbname + "_" + desc.query.String())

			t.Run(name, func(t *testing.T) {
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

					if !reflect.DeepEqual(res, desc.expected) {
            // Don't trust deep.Equal! It's only here to help, but is often wrong.
					  diff := deep.Equal(res, desc.expected)
						pretty.Println("actual", res)
						pretty.Println("expected", desc.expected)
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
}

func pick(vals ...interface{}) []*aql.ResultRow {
	out := []*aql.ResultRow{}
	for _, ival := range vals {
		res := pickres(ival)
		out = append(out, &aql.ResultRow{Value: res})
	}
	return out
}

func pickrow(vals ...interface{}) []*aql.QueryResult {
	out := []*aql.QueryResult{}
	for _, ival := range vals {
		out = append(out, pickres(ival))
	}
	return out
}

func pickres(ival interface{}) *aql.QueryResult {
	switch val := ival.(type) {
	case *aql.Vertex:
		return &aql.QueryResult{
			&aql.QueryResult_Vertex{val},
		}
	case *aql.Edge:
		return &aql.QueryResult{
			&aql.QueryResult_Edge{val},
		}
	default:
		panic("unknown")
	}
}

func vert(id, label string, d dat) *aql.Vertex {
	return &aql.Vertex{
		Gid:   id,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func values_(vals ...interface{}) []*aql.ResultRow {
	out := []*aql.ResultRow{}
	for _, val := range vals {
		out = append(out, &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					// TODO would be better if this didn't depend on protoutil,
					//      since that is a major part of what is being tested.
					protoutil.WrapValue(val),
				},
			},
		})
	}
	return out
}

func edge(id, from, to, label string, d dat) *aql.Edge {
	return &aql.Edge{
		Gid:   id,
		From:  from,
		To:    to,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

var rx = regexp.MustCompile(`[\(\),\. ]`)
var rx2 = regexp.MustCompile(`__*`)

func cleanName(name string) string {
	name = rx.ReplaceAllString(name, "_")
	name = rx2.ReplaceAllString(name, "_")
	name = strings.TrimSuffix(name, "_")
	return name
}

type dat map[string]interface{}
