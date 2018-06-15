package test

import (
	"context"
	"fmt"
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
			count(len(vertices)),
		},
		{
			Q.E().Count(),
			count(len(edges)),
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
			Q.V().Where(aql.Eq("_label", "users")).Out().Count(),
			count(100),
		},
		{
			Q.V("users:1").Out(),
			pick("purchases:57"),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).Out().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").Out(),
			pick("products:3", "products:8"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Out().Count(),
			count(0),
		},
		{
			Q.V("products:1").Out(),
			pick(),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).In().Count(),
			count(0),
		},
		{
			Q.V("users:1").In(),
			pick(),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).In().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").In(),
			pick("users:7"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).In().Count(),
			count(100),
		},
		{
			Q.V("products:1").In(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Both().Count(),
			count(100),
		},
		{
			Q.V("users:1").Both(),
			pick("purchases:57"),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).Both().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").Both(),
			pick("users:7", "products:3", "products:8"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Both().Count(),
			count(100),
		},
		{
			Q.V("products:1").Both(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).OutEdge().Count(),
			count(100),
		},
		{
			Q.V("users:1").OutEdge(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).OutEdge().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").OutEdge(),
			pick("purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).OutEdge().Count(),
			count(0),
		},
		{
			Q.V("products:1").OutEdge(),
			pick(),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).InEdge().Count(),
			count(0),
		},
		{
			Q.V("users:1").InEdge(),
			pick(),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).InEdge().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").InEdge(),
			pick("userPurchases:users:7:purchases:1"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).InEdge().Count(),
			count(100),
		},
		{
			Q.V("products:1").InEdge(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).BothEdge().Count(),
			count(100),
		},
		{
			Q.V("users:1").BothEdge(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).BothEdge().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").BothEdge(),
			pick("userPurchases:users:7:purchases:1", "purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).BothEdge().Count(),
			count(100),
		},
		{
			Q.V("products:1").BothEdge(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Count(),
			count(50),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Count(),
			count(20),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).Count(),
			count(100),
		},
		{
			Q.E().Where(aql.Eq("_label", "purchasedProducts")).Count(),
			count(100),
		},
		{
			Q.E().Where(aql.Eq("_label", "userPurchases")).Count(),
			count(100),
		},
		{
			Q.V().Where(aql.Eq("_label", "does-not-exist")).Count(),
			count(0),
		},
		{
			Q.E().Where(aql.Eq("_label", "does-not-exist")).Count(),
			count(0),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Where(aql.Eq("details", "\"sex\"=>\"M\"")).Count(),
			count(17),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Where(aql.Not(aql.Eq("details", "\"sex\"=>\"M\""))).Count(),
			count(33),
		},
		{
			Q.V().Where(aql.Eq("_label", "users")).Where(aql.Neq("details", "\"sex\"=>\"M\"")).Count(),
			count(33),
		},
		{
			Q.V().Where(aql.Eq("_label", "purchases")).Where(aql.Or(aql.Eq("state", "TX"), aql.Eq("state", "WY"))).Count(),
			count(19),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Eq("price", 29.99)),
			pick("products:2"),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Gt("price", 29.99)).Count(),
			count(5),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Gte("price", 29.99)).Count(),
			count(6),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Lt("price", 29.99)).Count(),
			count(14),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Lte("price", 29.99)).Count(),
			count(15),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.And(aql.Lt("price", 29.99), aql.Gt("price", 9.99))).Count(),
			count(6),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.Contains("tags", "Movie")).Count(),
			count(5),
		},
		{
			Q.V().Where(aql.Eq("_label", "products")).Where(aql.In("title", "Action", "Drama")),
			pick("products:19", "products:20"),
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

func pick(gids ...string) checker {
	expect := []*aql.QueryResult{}
	for _, id := range gids {
		res := pickgid(id)
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickgid(gid string) *aql.QueryResult {
	for _, v := range vertices {
		if v.Gid == gid {
			return &aql.QueryResult{
				Result: &aql.QueryResult_Vertex{Vertex: v},
			}
		}
	}
	for _, e := range edges {
		if e.Gid == gid {
			return &aql.QueryResult{
				Result: &aql.QueryResult_Edge{Edge: e},
			}
		}
	}
	panic("no vertex or edge found for gid")
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
		panic(fmt.Sprintf("unhandled type %T", ival))
	}
}

func pickAllVertices() checker {
	expect := []*aql.QueryResult{}
	for _, v := range vertices {
		res := &aql.QueryResult{
			Result: &aql.QueryResult_Vertex{Vertex: v},
		}
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickAllEdges() checker {
	expect := []*aql.QueryResult{}
	for _, e := range edges {
		res := &aql.QueryResult{
			Result: &aql.QueryResult_Edge{Edge: e},
		}
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

func count(i int) checker {
	expect := []*aql.QueryResult{
		{
			Result: &aql.QueryResult_Count{
				Count: uint32(i),
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
