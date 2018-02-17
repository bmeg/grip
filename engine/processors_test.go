package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	_ "github.com/bmeg/arachne/boltdb"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/memgraph"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/jsonpb"
	"github.com/rs/xid"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

var Q = aql.Query{}

var verts = []*aql.Vertex{
	vert("Human", dat{"name": "Alex"}),
	vert("Human", dat{"name": "Kyle"}),
	vert("Human", dat{"name": "Ryan"}),
	vert("Robot", dat{"name": "C-3PO"}),
	vert("Robot", dat{"name": "R2-D2"}),
	vert("Robot", dat{"name": "Bender"}),
	vert("Clone", dat{"name": "Alex"}),
	vert("Clone", dat{"name": "Kyle"}),
	vert("Clone", dat{"name": "Ryan"}),
	vert("Clone", nil),
	vert("Project", dat{"name": "Funnel"}),
	vert("Project", dat{"name": "Gaia"}),
}

var edges = []*aql.Edge{
	edge(verts[0], verts[10], "WorksOn", nil),
	edge(verts[2], verts[11], "WorksOn", nil),
}

var table = []struct {
	query    *aql.Query
	expected checker
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
		pick(verts[0], verts[1], verts[2]),
	},
	{
		Q.V().HasLabel("Robot"),
		pick(verts[3], verts[4], verts[5]),
	},
	{
		Q.V().HasLabel("Robot", "Human"),
		pick(verts[0], verts[1], verts[2], verts[3], verts[4], verts[5]),
	},
	{
		Q.V().HasLabel("non-existant"),
		pick(),
	},
	{
		Q.V().HasID(verts[0].Gid, verts[2].Gid),
		pick(verts[0], verts[2]),
	},
	{
		Q.V().HasID("non-existant"),
		pick(),
	},
	{
		Q.V().Limit(2),
		func(t *testing.T, res []*aql.ResultRow) {
			if len(res) != 2 {
				t.Error("expected 2 results")
			}
		},
	},
	{
		Q.V().Count(),
		// TODO wrong. should be int.
		values_(float64(len(verts))),
	},
	{
		Q.V().HasLabel("Human").Has("name", "Ryan"),
		pick(verts[2]),
	},
	{
		Q.V().HasLabel("Human").As("x").Has("name", "Alex").Select("x"),
		pick(verts[0]),
	},
	{
		Q.V(),
		pickAllVerts(),
	},
	{
		Q.E(),
		pickAllEdges(),
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
		pickAllEdges(),
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
		Q.V().HasLabel("Human").Values("name"),
		values_("Alex", "Kyle", "Ryan"),
	},
	{
		Q.V().HasLabel("Human").Values(),
		values_(verts[0].Data, verts[1].Data, verts[2].Data),
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
		).Select("b", "c"),
		pickrow(verts[0], verts[0]),
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
		"mem":    memgraph.NewMemGraph(),
		"bolt":   bolt.Graph("test-graph"),
		"badger": badgerdb.NewBadgerArachne("test-badger.db").Graph("test-graph"),
	}

	for dbname, db := range dbs {

		for _, v := range verts {
			db.AddVertex(v)
		}
		for _, e := range edges {
			db.AddEdge(e)
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

					desc.expected(t, res)
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

// checker is the interface of a function that validates the results of a test query.
type checker func(t *testing.T, actual []*aql.ResultRow)

// this sorts the results to account for non-determinstic ordering from the db.
// TODO this will break sort tests
func compare(expect []*aql.ResultRow) checker {
	return func(t *testing.T, actual []*aql.ResultRow) {
		mar := jsonpb.Marshaler{}
		actualS := []string{}
		expectS := []string{}

		for _, r := range actual {
			s, _ := mar.MarshalToString(r)
			actualS = append(actualS, s)
		}
		for _, r := range expect {
			s, _ := mar.MarshalToString(r)
			expectS = append(expectS, s)
		}

		sort.Strings(actualS)
		sort.Strings(expectS)

		if !reflect.DeepEqual(actualS, expectS) {
			for _, s := range actualS {
				t.Log("actual", s)
			}
			for _, s := range expectS {
				t.Log("expect", s)
			}
			t.Error("not equal")
		}
	}
}

func pick(vals ...interface{}) checker {
	expect := []*aql.ResultRow{}
	for _, ival := range vals {
		res := pickres(ival)
		expect = append(expect, &aql.ResultRow{Value: res})
	}
	return compare(expect)
}

func pickrow(vals ...interface{}) checker {
	expect := []*aql.QueryResult{}
	for _, ival := range vals {
		expect = append(expect, pickres(ival))
	}
	return compare([]*aql.ResultRow{
		{Row: expect},
	})
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

func pickAllVerts() checker {
	expect := []*aql.ResultRow{}
	for _, ival := range verts {
		res := pickres(ival)
		expect = append(expect, &aql.ResultRow{Value: res})
	}
	return compare(expect)
}

func pickAllEdges() checker {
	expect := []*aql.ResultRow{}
	for _, ival := range edges {
		res := pickres(ival)
		expect = append(expect, &aql.ResultRow{Value: res})
	}
	return compare(expect)
}

func values_(vals ...interface{}) checker {
	expect := []*aql.ResultRow{}
	for _, val := range vals {
		expect = append(expect, &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					// TODO would be better if this didn't depend on protoutil,
					//      since that is a major part of what is being tested.
					protoutil.WrapValue(val),
				},
			},
		})
	}
	return compare(expect)
}

func vert(label string, d dat) *aql.Vertex {
	return &aql.Vertex{
		Gid:   xid.New().String(),
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(from, to *aql.Vertex, label string, d dat) *aql.Edge {
	return &aql.Edge{
		Gid:   xid.New().String(),
		From:  from.Gid,
		To:    to.Gid,
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
