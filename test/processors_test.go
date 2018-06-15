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
	// "github.com/bmeg/arachne/protoutil"
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

func TestEngine(t *testing.T) {
	tests := []queryTest{
		{
			Q.V().Count(),
			count(uint32(len(vertices))),
		},
		{
			Q.E().Count(),
			count(uint32(len(edges))),
		},
		{
			Q.V().Where(aql.Eq("non-existent-field", "foobar")).Count(),
			count(uint32(0)),
		},
		{
			Q.E().Where(aql.Eq("non-existent-field", "foobar")).Count(),
			count(uint32(0)),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Count(),
			count(uint32(50)),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Count(),
			count(uint32(20)),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).Count(),
			count(uint32(100)),
		},
		{
			Q.V(),
			pickAllVertices(),
		},
		{
			Q.E(),
			pickAllEdges(),
		},
	}

	for _, desc := range tests {
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
			if len(expectS) != len(actualS) {
				t.Logf("expected # results: %d actual # results: %d", len(expectS), len(actualS))
			}
			t.Errorf("not equal")
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
