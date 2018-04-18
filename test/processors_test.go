package test

import (
	"context"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/protoutil"
	"github.com/golang/protobuf/jsonpb"
)

var Q = &aql.Query{}

var verts = []*aql.Vertex{
	vert("Human", dat{"name": "Alex", "age": 12}),
	vert("Human", dat{"name": "Kyle", "age": 34}),
	vert("Human", dat{"name": "Ryan", "age": 56}),
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

// checker is the interface of a function that validates the results of a test query.
type checker func(t *testing.T, actual <-chan *aql.ResultRow)

type queryTest struct {
	query    *aql.Query
	expected checker
}

var table = []queryTest{
	{
		Q.V().Has("name", "Kyle", "Alex"),
		pick(verts[0], verts[1], verts[6], verts[7]),
	},
	{
		Q.V().Has("non-existent", "Kyle", "Alex"),
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
		Q.V().HasLabel("non-existent"),
		pick(),
	},
	{
		Q.V().HasID(verts[0].Gid, verts[2].Gid),
		pick(verts[0], verts[2]),
	},
	{
		Q.V().HasID("non-existent"),
		pick(),
	},
	{
		Q.V().Limit(2),
		func(t *testing.T, res <-chan *aql.ResultRow) {
			count := 0
			for range res {
				count++
			}
			if count != 2 {
				t.Errorf("expected 2 results got %v", count)
			}
		},
	},
	{
		Q.V().Count(),
		// TODO wrong. should be int.
		values(float64(len(verts))),
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
		values(verts[6].Data, verts[7].Data, verts[8].Data),
	},
	{
		Q.V().HasLabel("Human").Values(),
		values(verts[0].Data, verts[1].Data, verts[2].Data),
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
	for _, dbname := range []string{"badger", "bolt", "level", "rocks"} {
		dbpath := "test.db." + randomString(6)
		defer os.RemoveAll(dbpath)

		kvg, err := kvgraph.NewKVGraphDB(dbname, dbpath)
		if err != nil {
			t.Fatal(err)
		}

		err = kvg.AddGraph("test-graph")
		if err != nil {
			t.Fatal(err)
		}

		db := kvg.Graph("test-graph")

		for _, v := range verts {
			err := db.AddVertex([]*aql.Vertex{v})
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, e := range edges {
			err := db.AddEdge([]*aql.Edge{e})
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, desc := range table {
			desc := desc
			db := db
			name := cleanName(dbname + "_" + desc.query.String())

			t.Run(name, func(t *testing.T) {
				p, err := db.Compiler().Compile(desc.query.Statements)
				if err != nil {
					t.Fatal(err)
				}
				res := engine.Run(context.Background(), p, "./workdir")
				desc.expected(t, res)
			})
		}
	}
}

// this sorts the results to account for non-determinstic ordering from the db.
// TODO this will break sort tests
func compare(expect []*aql.ResultRow) checker {
	return func(t *testing.T, actual <-chan *aql.ResultRow) {
		mar := jsonpb.Marshaler{}
		actualS := []string{}
		expectS := []string{}
		for r := range actual {
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
			Result: &aql.QueryResult_Vertex{Vertex: val},
		}
	case *aql.Edge:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Edge{Edge: val},
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

func values(vals ...interface{}) checker {
	expect := []*aql.ResultRow{}
	for _, val := range vals {
		expect = append(expect, &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Data{
					// TODO would be better if this didn't depend on protoutil,
					//      since that is a major part of what is being tested.
					Data: protoutil.WrapValue(val),
				},
			},
		})
	}
	return compare(expect)
}

func vert(label string, d dat) *aql.Vertex {
	return &aql.Vertex{
		Gid:   randomString(10),
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(from, to *aql.Vertex, label string, d dat) *aql.Edge {
	return &aql.Edge{
		Gid:   randomString(10),
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
