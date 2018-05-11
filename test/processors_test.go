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
	"github.com/bmeg/arachne/protoutil"
	"github.com/bmeg/arachne/util"
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
type checker func(t *testing.T, actual <-chan *aql.QueryResult)

type queryTest struct {
	query    *aql.Query
	expected checker
}

var table = []queryTest{
	{
		Q.V().Where(aql.In("$.name", "Kyle", "Alex")),
		pick(verts[0], verts[1], verts[6], verts[7]),
	},
	{
		Q.V().Where(aql.Eq("$.non-existent-field", "Kyle")),
		pick(),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")),
		pick(verts[0], verts[1], verts[2]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Robot")),
		pick(verts[3], verts[4], verts[5]),
	},
	{
		Q.V().Where(aql.In("$.label", "Robot", "Human")),
		pick(verts[0], verts[1], verts[2], verts[3], verts[4], verts[5]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "non-existent")),
		pick(),
	},
	{
		Q.V().Where(aql.In("$.gid", verts[0].Gid, verts[2].Gid)),
		pick(verts[0], verts[2]),
	},
	{
		Q.V().Where(aql.Eq("$.gid", "non-existent")),
		pick(),
	},
	{
		Q.V().Limit(2),
		func(t *testing.T, res <-chan *aql.QueryResult) {
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
		count(uint32(len(verts))),
	},
	{
		Q.V().Where(aql.And(aql.Eq("$.label", "Human"), aql.Eq("$.name", "Ryan"))),
		pick(verts[2]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).Mark("x").Where(aql.Eq("$.name", "Alex")).Select("x"),
		pickselection(map[string]interface{}{"x": verts[0]}),
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
		Q.V().Where(aql.Eq("$.label", "Human")).Out(),
		pick(verts[10], verts[11]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).Out().Where(aql.Eq("$.name", "Funnel")),
		pick(verts[10]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).Mark("x").Out().Where(aql.Eq("$.name", "Funnel")).Select("x"),
		pickselection(map[string]interface{}{"x": verts[0]}),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).OutEdge(),
		pickAllEdges(),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).Where(aql.Eq("$.name", "Alex")).OutEdge(),
		pick(edges[0]),
	},
	{
		Q.V().Where(aql.Eq("$.label", "Human")).Fields("$.name"),
		pick(
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Alex"})},
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Kyle"})},
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Ryan"})},
		),
	},
	{
		Q.V().
			Where(aql.Eq("$.label", "Human")).Mark("x").
			Out().
			Where(aql.Eq("$.name", "Funnel")).Mark("y").
			Fields("$y.gid", "$y.label", "$y.name", "$x.gid", "$x.label", "$x.name").
			Select("x", "y"),
		pickselection(map[string]interface{}{
			"x": &aql.Vertex{Gid: verts[0].Gid, Label: verts[0].Label, Data: protoutil.AsStruct(map[string]interface{}{"name": "Alex"})},
			"y": &aql.Vertex{Gid: verts[10].Gid, Label: verts[10].Label, Data: protoutil.AsStruct(map[string]interface{}{"name": "Funnel"})},
		}),
	},
	{
		Q.V().Match(
			Q.Where(aql.Eq("$.label", "Human")),
			Q.Where(aql.Eq("$.name", "Alex")),
		),
		pick(verts[0]),
	},
	{
		Q.V().Match(
			Q.Mark("a").Where(aql.Eq("$.label", "Human")).Mark("b"),
			Q.Mark("b").Where(aql.Eq("$.name", "Alex")).Mark("c"),
		).Select("c"),
		pickselection(map[string]interface{}{"c": verts[0]}),
	},
	{
		Q.V().Match(
			Q.Mark("a").Where(aql.Eq("$.label", "Human")).Mark("b"),
			Q.Mark("b").Where(aql.Eq("$.name", "Alex")).Mark("c"),
		).Select("b", "c"),
		pickselection(map[string]interface{}{"b": verts[0], "c": verts[0]}),
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
		name := cleanName(dbname + "_" + desc.query.String())

		t.Run(name, func(t *testing.T) {
			pipeline, err := db.Compiler().Compile(desc.query.Statements)
			if err != nil {
				t.Fatal(err)
			}
			workdir := "./test.workdir." + util.RandomString(6)
			defer os.RemoveAll(workdir)
			res := engine.Run(context.Background(), pipeline, workdir)
			desc.expected(t, res)
		})
	}
}

// this sorts the results to account for non-determinstic ordering from the db.
// TODO this will break sort tests
func compare(expect []*aql.QueryResult) checker {
	return func(t *testing.T, actual <-chan *aql.QueryResult) {
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
	expect := []*aql.QueryResult{}
	for _, ival := range vals {
		res := pickres(ival)
		expect = append(expect, res)
	}
	return compare(expect)
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
	expect := []*aql.QueryResult{}
	for _, ival := range verts {
		res := pickres(ival)
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickAllEdges() checker {
	expect := []*aql.QueryResult{}
	for _, ival := range edges {
		res := pickres(ival)
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickselection(selection map[string]interface{}) checker {
	s := map[string]*aql.Selection{}
	for mark, ival := range selection {
		switch val := ival.(type) {
		case *aql.Vertex:
			s[mark] = &aql.Selection{
				Result: &aql.Selection_Vertex{
					Vertex: val,
				},
			}
		case *aql.Edge:
			s[mark] = &aql.Selection{
				Result: &aql.Selection_Edge{
					Edge: val,
				},
			}
		default:
			panic("unknown")
		}
	}
	expect := []*aql.QueryResult{
		{
			Result: &aql.QueryResult_Selections{
				Selections: &aql.Selections{Selections: s},
			},
		},
	}
	return compare(expect)
}

func count(i uint32) checker {
	expect := []*aql.QueryResult{
		{
			Result: &aql.QueryResult_Count{
				Count: i,
			},
		},
	}
	return compare(expect)
}

func vert(label string, d dat) *aql.Vertex {
	return &aql.Vertex{
		Gid:   util.UUID(),
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(from, to *aql.Vertex, label string, d dat) *aql.Edge {
	return &aql.Edge{
		Gid:   util.UUID(),
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
