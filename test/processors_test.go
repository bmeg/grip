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

// checker is the interface of a function that validates the results of a test query.
type checker func(t *testing.T, actual <-chan *aql.QueryResult)

type queryTest struct {
	query    *aql.Query
	expected checker
}

var table = []queryTest{
	{
		Q.V().Where(aql.In("name", "Kyle", "Alex")),
		pick(vertices[0], vertices[1], vertices[6], vertices[7]),
	},
	{
		Q.V().Where(aql.Eq("non-existent-field", "Kyle")),
		pick(),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")),
		pick(vertices[0], vertices[1], vertices[2]),
	},
	{
		Q.V().Where(aql.Eq("_label", "Robot")),
		pick(vertices[3], vertices[4], vertices[5]),
	},
	{
		Q.V().Where(aql.In("_label", "Robot", "Human")),
		pick(vertices[0], vertices[1], vertices[2], vertices[3], vertices[4], vertices[5]),
	},
	{
		Q.V().Where(aql.Eq("_label", "non-existent")),
		pick(),
	},
	{
		Q.V().Where(aql.In("_gid", vertices[0].Gid, vertices[2].Gid)),
		pick(vertices[0], vertices[2]),
	},
	{
		Q.V().Where(aql.Eq("_gid", "non-existent")),
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
		count(uint32(len(vertices))),
	},
	{
		Q.V().Where(aql.And(aql.Eq("_label", "Human"), aql.Eq("name", "Ryan"))),
		pick(vertices[2]),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Mark("x").Where(aql.Eq("name", "Alex")).Select("x"),
		pickselection(map[string]interface{}{"x": vertices[0]}),
	},
	{
		Q.V(),
		pickAllVertices(),
	},
	{
		Q.E(),
		pickAllEdges(),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Out(),
		pick(vertices[10], vertices[11]),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Out().Where(aql.Eq("name", "Funnel")),
		pick(vertices[10]),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Mark("x").Out().Where(aql.Eq("name", "Funnel")).Select("x"),
		pickselection(map[string]interface{}{"x": vertices[0]}),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).OutEdge(),
		pickAllEdges(),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Where(aql.Eq("name", "Alex")).OutEdge(),
		pick(edges[0]),
	},
	{
		Q.V().Where(aql.Eq("_label", "Human")).Fields("name"),
		pick(
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Alex"})},
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Kyle"})},
			&aql.Vertex{Data: protoutil.AsStruct(map[string]interface{}{"name": "Ryan"})},
		),
	},
	{
		Q.V().
			Where(aql.Eq("_label", "Human")).Mark("x").
			Out().
			Where(aql.Eq("name", "Funnel")).Mark("y").
			Fields("$y._gid", "$y._label", "$y.name", "$x._gid", "$x._label", "$x.name").
			Select("x", "y"),
		pickselection(map[string]interface{}{
			"x": &aql.Vertex{Gid: vertices[0].Gid, Label: vertices[0].Label, Data: protoutil.AsStruct(map[string]interface{}{"name": "Alex"})},
			"y": &aql.Vertex{Gid: vertices[10].Gid, Label: vertices[10].Label, Data: protoutil.AsStruct(map[string]interface{}{"name": "Funnel"})},
		}),
	},
	{
		Q.V().Match(
			Q.Where(aql.Eq("_label", "Human")),
			Q.Where(aql.Eq("name", "Alex")),
		),
		pick(vertices[0]),
	},
	{
		Q.V().Match(
			Q.Mark("a").Where(aql.Eq("_label", "Human")).Mark("b"),
			Q.Mark("b").Where(aql.Eq("name", "Alex")).Mark("c"),
		).Select("c"),
		pickselection(map[string]interface{}{"c": vertices[0]}),
	},
	{
		Q.V().Match(
			Q.Mark("a").Where(aql.Eq("_label", "Human")).Mark("b"),
			Q.Mark("b").Where(aql.Eq("name", "Alex")).Mark("c"),
		).Select("b", "c"),
		pickselection(map[string]interface{}{"b": vertices[0], "c": vertices[0]}),
	},
}

func TestEngine(t *testing.T) {
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

// This sorts the results to account for non-determinstic ordering from the db.
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

func pickAllVertices() checker {
	expect := []*aql.QueryResult{}
	for _, ival := range vertices {
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

func cleanName(name string) string {
	rx := regexp.MustCompile(`[\(\),\. ]`)
	rx2 := regexp.MustCompile(`__*`)
	name = rx.ReplaceAllString(name, "_")
	name = rx2.ReplaceAllString(name, "_")
	name = strings.TrimSuffix(name, "_")
	return name
}
