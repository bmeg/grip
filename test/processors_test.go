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

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
	"github.com/golang/protobuf/jsonpb"
)

var Q = &gripql.Query{}

// checker is the interface of a function that validates the results of a test query.
type checker func(t *testing.T, actual <-chan *gripql.QueryResult)

type queryTest struct {
	query    *gripql.Query
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
			Q.V().Where(gripql.Eq("_label", "users")).Count(),
			count(50),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Count(),
			count(20),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).Count(),
			count(100),
		},
		{
			Q.E().Where(gripql.Eq("_label", "purchasedProducts")).Count(),
			count(100),
		},
		{
			Q.E().Where(gripql.Eq("_label", "userPurchases")).Count(),
			count(100),
		},
		{
			Q.V().Where(gripql.Eq("_label", "does-not-exist")).Count(),
			count(0),
		},
		{
			Q.E().Where(gripql.Eq("_label", "does-not-exist")).Count(),
			count(0),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).Out().Count(),
			count(100),
		},
		{
			Q.V("users:1").Out(),
			pick("purchases:57"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).Out().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").Out(),
			pick("products:3", "products:8"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Out().Count(),
			count(0),
		},
		{
			Q.V("products:1").Out(),
			pick(),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).In().Count(),
			count(0),
		},
		{
			Q.V("users:1").In(),
			pick(),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).In().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").In(),
			pick("users:7"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).In().Count(),
			count(100),
		},
		{
			Q.V("products:1").In(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).Both().Count(),
			count(100),
		},
		{
			Q.V("users:1").Both(),
			pick("purchases:57"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).Both().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").Both(),
			pick("users:7", "products:3", "products:8"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Both().Count(),
			count(100),
		},
		{
			Q.V("products:1").Both(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).OutEdge().Count(),
			count(100),
		},
		{
			Q.V("users:1").OutEdge(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).OutEdge().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").OutEdge(),
			pick("purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).OutEdge().Count(),
			count(0),
		},
		{
			Q.V("products:1").OutEdge(),
			pick(),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).InEdge().Count(),
			count(0),
		},
		{
			Q.V("users:1").InEdge(),
			pick(),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).InEdge().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").InEdge(),
			pick("userPurchases:users:7:purchases:1"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).InEdge().Count(),
			count(100),
		},
		{
			Q.V("products:1").InEdge(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).BothEdge().Count(),
			count(100),
		},
		{
			Q.V("users:1").BothEdge(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).BothEdge().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").BothEdge(),
			pick("userPurchases:users:7:purchases:1", "purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).BothEdge().Count(),
			count(100),
		},
		{
			Q.V("products:1").BothEdge(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).Where(gripql.Eq("details", "\"sex\"=>\"M\"")).Count(),
			count(17),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).Where(gripql.Not(gripql.Eq("details", "\"sex\"=>\"M\""))).Count(),
			count(33),
		},
		{
			Q.V().Where(gripql.Eq("_label", "users")).Where(gripql.Neq("details", "\"sex\"=>\"M\"")).Count(),
			count(33),
		},
		{
			Q.V().Where(gripql.Eq("_label", "purchases")).Where(gripql.Or(gripql.Eq("state", "TX"), gripql.Eq("state", "WY"))).Count(),
			count(19),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Eq("price", 29.99)),
			pick("products:2"),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Gt("price", 29.99)).Count(),
			count(5),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Gte("price", 29.99)).Count(),
			count(6),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Lt("price", 29.99)).Count(),
			count(14),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Lte("price", 29.99)).Count(),
			count(15),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.And(gripql.Lt("price", 29.99), gripql.Gt("price", 9.99))).Count(),
			count(6),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.Contains("tags", "Movie")).Count(),
			count(5),
		},
		{
			Q.V().Where(gripql.Eq("_label", "products")).Where(gripql.In("title", "Action", "Drama")),
			pick("products:19", "products:20"),
		},
		{
			Q.V().Limit(10).Count(),
			count(10),
		},
		{
			Q.V().Offset(100).Count(),
			count(70),
		},
		{
			Q.V("users:1").Fields("_label", "email"),
			pickRes(vertex("", "users", data{"email": "Earlean.Bonacci@yahoo.com"})),
		},
		{
			Q.V("users:1").Mark("a").Out().Mark("b").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": getVertex("users:1"),
				"b": getVertex("purchases:57"),
			}),
		},
		{
			Q.V("users:1").Mark("a").Out().Mark("b").Fields("$a._gid", "$a._label", "$b._gid", "$b._label").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": vertex("users:1", "users", nil),
				"b": vertex("purchases:57", "purchases", nil),
			}),
		},
		{
			Q.V().Match(
				Q.Where(gripql.Eq("_label", "products")),
				Q.Where(gripql.Eq("price", 499.99)),
			),
			pick("products:6"),
		},
		{
			Q.V().Match(
				Q.Mark("a").Where(gripql.Eq("_label", "products")).Mark("b"),
				Q.Mark("b").Where(gripql.Eq("price", 499.99)).Mark("c"),
			).Select("c"),
			pickSelection(map[string]interface{}{
				"c": getVertex("products:6"),
			}),
		},
		{
			Q.V("users:1").Mark("a").Out().Mark("b").
				Render(map[string]interface{}{"user_id": "$a._gid", "purchase_id": "$b._gid", "purchaser": "$b.name"}),
			render(map[string]interface{}{"user_id": "users:1", "purchase_id": "purchases:57", "purchaser": "Letitia Sprau"}),
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

func vertex(gid, label string, d data) *gripql.Vertex {
	return &gripql.Vertex{
		Gid:   gid,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(gid interface{}, from, to string, label string, d data) *gripql.Edge {
	return &gripql.Edge{
		Gid:   fmt.Sprintf("%v", gid),
		From:  from,
		To:    to,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

type data map[string]interface{}

// This sorts the results to account for non-determinstic ordering from the db.
// TODO this will break sort tests
func compare(expect []*gripql.QueryResult) checker {
	return func(t *testing.T, actual <-chan *gripql.QueryResult) {
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
	expect := []*gripql.QueryResult{}
	for _, id := range gids {
		res := pickgid(id)
		expect = append(expect, res)
	}
	return compare(expect)
}

func getVertex(gid string) *gripql.Vertex {
	for _, v := range vertices {
		if v.Gid == gid {
			return v
		}
	}
	return nil
}

func getEdge(gid string) *gripql.Edge {
	for _, e := range edges {
		if e.Gid == gid {
			return e
		}
	}
	return nil
}

func pickgid(gid string) *gripql.QueryResult {
	v := getVertex(gid)
	if v != nil {
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Vertex{Vertex: v},
		}
	}
	e := getEdge(gid)
	if e != nil {
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Edge{Edge: e},
		}
	}
	panic("no vertex or edge found for gid")
}

func pickRes(ival ...interface{}) checker {
	expect := []*gripql.QueryResult{}
	for _, val := range ival {
		switch v := val.(type) {
		case *gripql.Vertex:
			res := &gripql.QueryResult{
				Result: &gripql.QueryResult_Vertex{Vertex: v},
			}
			expect = append(expect, res)
		case *gripql.Edge:
			res := &gripql.QueryResult{
				Result: &gripql.QueryResult_Edge{Edge: v},
			}
			expect = append(expect, res)
		default:
			panic(fmt.Sprintf("unhandled type %T", val))
		}
	}
	return compare(expect)
}

func pickAllVertices() checker {
	expect := []*gripql.QueryResult{}
	for _, v := range vertices {
		res := &gripql.QueryResult{
			Result: &gripql.QueryResult_Vertex{Vertex: v},
		}
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickAllEdges() checker {
	expect := []*gripql.QueryResult{}
	for _, e := range edges {
		res := &gripql.QueryResult{
			Result: &gripql.QueryResult_Edge{Edge: e},
		}
		expect = append(expect, res)
	}
	return compare(expect)
}

func pickSelection(selection map[string]interface{}) checker {
	s := map[string]*gripql.Selection{}
	for mark, ival := range selection {
		switch val := ival.(type) {
		case *gripql.Vertex:
			s[mark] = &gripql.Selection{
				Result: &gripql.Selection_Vertex{
					Vertex: val,
				},
			}
		case *gripql.Edge:
			s[mark] = &gripql.Selection{
				Result: &gripql.Selection_Edge{
					Edge: val,
				},
			}
		default:
			panic(fmt.Sprintf("unhandled type %T", ival))
		}
	}
	expect := []*gripql.QueryResult{
		{
			Result: &gripql.QueryResult_Selections{
				Selections: &gripql.Selections{Selections: s},
			},
		},
	}
	return compare(expect)
}

func count(i int) checker {
	expect := []*gripql.QueryResult{
		{
			Result: &gripql.QueryResult_Count{
				Count: uint32(i),
			},
		},
	}
	return compare(expect)
}

func render(v interface{}) checker {
	expect := []*gripql.QueryResult{
		{
			Result: &gripql.QueryResult_Render{
				Render: protoutil.WrapValue(v),
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
