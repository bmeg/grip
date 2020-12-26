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

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/protobuf/types/known/structpb"
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
			Q.V().HasLabel("users").Count(),
			count(50),
		},
		{
			Q.V().HasLabel("products").Count(),
			count(20),
		},
		{
			Q.V().HasLabel("purchases").Count(),
			count(100),
		},
		{
			Q.V().HasLabel("users", "products", "purchases").Count(),
			count(170),
		},
		{
			Q.E().HasLabel("purchasedProducts").Count(),
			count(100),
		},
		{
			Q.E().HasLabel("userPurchases").Count(),
			count(100),
		},
		{
			Q.V().HasLabel("does-not-exist").Count(),
			count(0),
		},
		{
			Q.E().HasLabel("does-not-exist").Count(),
			count(0),
		},
		{
			Q.V().HasLabel("users").Out().Count(),
			count(100),
		},
		{
			Q.V("users:1").Out(),
			pick("purchases:57"),
		},
		{
			Q.V().HasLabel("purchases").Out().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").Out(),
			pick("products:3", "products:8"),
		},
		{
			Q.V().HasLabel("products").Out().Count(),
			count(0),
		},
		{
			Q.V("products:1").Out(),
			pick(),
		},
		{
			Q.V().HasLabel("users").In().Count(),
			count(0),
		},
		{
			Q.V("users:1").In(),
			pick(),
		},
		{
			Q.V().HasLabel("purchases").In().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").In(),
			pick("users:7"),
		},
		{
			Q.V().HasLabel("products").In().Count(),
			count(100),
		},
		{
			Q.V("products:1").In(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().HasLabel("users").Both().Count(),
			count(100),
		},
		{
			Q.V("users:1").Both(),
			pick("purchases:57"),
		},
		{
			Q.V().HasLabel("purchases").Both().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").Both(),
			pick("users:7", "products:3", "products:8"),
		},
		{
			Q.V().HasLabel("products").Both().Count(),
			count(100),
		},
		{
			Q.V("products:1").Both(),
			pick("purchases:2", "purchases:19", "purchases:34", "purchases:59", "purchases:60"),
		},
		{
			Q.V().HasLabel("users").OutE().Count(),
			count(100),
		},
		{
			Q.V("users:1").OutE(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().HasLabel("purchases").OutE().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").OutE(),
			pick("purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().HasLabel("products").OutE().Count(),
			count(0),
		},
		{
			Q.V("products:1").OutE(),
			pick(),
		},
		{
			Q.V().HasLabel("users").InE().Count(),
			count(0),
		},
		{
			Q.V("users:1").InE(),
			pick(),
		},
		{
			Q.V().HasLabel("purchases").InE().Count(),
			count(100),
		},
		{
			Q.V("purchases:1").InE(),
			pick("userPurchases:users:7:purchases:1"),
		},
		{
			Q.V().HasLabel("products").InE().Count(),
			count(100),
		},
		{
			Q.V("products:1").InE(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().HasLabel("users").BothE().Count(),
			count(100),
		},
		{
			Q.V("users:1").BothE(),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V().HasLabel("purchases").BothE().Count(),
			count(200),
		},
		{
			Q.V("purchases:1").BothE(),
			pick("userPurchases:users:7:purchases:1", "purchase_items:2", "purchase_items:3"),
		},
		{
			Q.V().HasLabel("products").BothE().Count(),
			count(100),
		},
		{
			Q.V("products:1").BothE(),
			pick("purchase_items:4", "purchase_items:30", "purchase_items:56", "purchase_items:88", "purchase_items:89"),
		},
		{
			Q.V().HasLabel("users").Has(gripql.Eq("details", "\"sex\"=>\"M\"")).Count(),
			count(17),
		},
		{
			Q.V().HasLabel("users").Has(gripql.Not(gripql.Eq("details", "\"sex\"=>\"M\""))).Count(),
			count(33),
		},
		{
			Q.V().HasLabel("users").Has(gripql.Neq("details", "\"sex\"=>\"M\"")).Count(),
			count(33),
		},
		{
			Q.V().HasLabel("purchases").Has(gripql.Or(gripql.Eq("state", "TX"), gripql.Eq("state", "WY"))).Count(),
			count(19),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Eq("price", 29.99)),
			pick("products:2"),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Gt("price", 29.99)).Count(),
			count(5),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Gte("price", 29.99)).Count(),
			count(6),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Lt("price", 29.99)).Count(),
			count(14),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Lte("price", 29.99)).Count(),
			count(15),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Inside("price", []interface{}{9.99, 19.99})).Count(),
			count(5),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Between("price", []interface{}{9.99, 19.99})).Count(),
			count(11),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Outside("price", []interface{}{9.99, 19.99})).Count(),
			count(9),
		},
		{
			Q.V().HasLabel("products").Has(gripql.And(gripql.Lt("price", 29.99), gripql.Gt("price", 9.99))).Count(),
			count(6),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Contains("tags", "Movie")).Count(),
			count(5),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Within("title", "Action", "Drama")),
			pick("products:19", "products:20"),
		},
		{
			Q.V().HasLabel("products").Has(gripql.Without("title", "Action", "Drama")).Count(),
			count(18),
		},
		{
			Q.V().Limit(10).Count(),
			count(10),
		},
		{
			Q.V().Skip(100).Count(),
			count(70),
		},
		{
			Q.V().Range(10, 50).Count(),
			count(40),
		},
		{
			Q.V("users:1").Fields(),
			pickRes(vertex("users:1", "users", data{})),
		},
		{
			Q.V("users:1").Fields("email", "id"),
			pickRes(vertex("users:1", "users", data{"email": "Earlean.Bonacci@yahoo.com", "id": 1})),
		},
		{
			Q.V("users:1").Fields("-password", "email", "id"),
			pickRes(vertex("users:1", "users", data{"email": "Earlean.Bonacci@yahoo.com", "id": 1})),
		},
		{
			Q.V("users:1").Fields("-_gid", "-_label", "email", "id"),
			pickRes(vertex("", "", data{"email": "Earlean.Bonacci@yahoo.com", "id": 1})),
		},
		{
			Q.V("users:1").Fields("-created_at", "-deleted_at", "-details"),
			pickRes(vertex("users:1", "users", data{
				"email":    "Earlean.Bonacci@yahoo.com",
				"id":       1,
				"password": "029761dd44fec0b14825843ad0dfface",
			},
			)),
		},
		{
			Q.V("users:1").Fields("-_label"),
			pickRes(vertex("users:1", "", data{
				"created_at": "2009-12-20 20:36:00 +0000 UTC",
				"deleted_at": nil,
				"details":    nil,
				"email":      "Earlean.Bonacci@yahoo.com",
				"id":         1,
				"password":   "029761dd44fec0b14825843ad0dfface",
			},
			)),
		},
		{
			Q.V("users:1").As("a").Out().As("b").Select("a"),
			pick("users:1"),
		},
		{
			Q.V("users:1").As("a").OutE().As("b").Out().As("c").Select("b"),
			pick("userPurchases:users:1:purchases:57"),
		},
		{
			Q.V("users:11").As("a").OutE().As("b").Out().Select("b").Count(),
			count(2),
		},
		{
			Q.V("users:11").As("a").OutE().As("b").Out().Has(gripql.Neq("_gid", "purchases:4")).Select("b").Count(),
			count(1),
		},
		{
			Q.V("users:11").As("a").OutE().As("b").Out().Has(gripql.Neq("_gid", "purchases:4")).Select("b").Out(),
			pick("purchases:26"),
		},
		{
			Q.V("users:1").As("a").Out().As("b").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": getVertex("users:1"),
				"b": getVertex("purchases:57"),
			}),
		},
		{
			Q.V("users:1").Fields().As("a").Out().Fields().As("b").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": vertex("users:1", "users", nil),
				"b": vertex("purchases:57", "purchases", nil),
			}),
		},
		{
			Q.V("users:1").Fields("-created_at", "-deleted_at", "-details", "-id", "-password").As("a").Out().Fields().As("b").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": vertex("users:1", "users", data{"email": "Earlean.Bonacci@yahoo.com"}),
				"b": vertex("purchases:57", "purchases", nil),
			}),
		},
		{
			Q.V("users:1").Fields().As("a").Out().Fields("state").As("b").Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": vertex("users:1", "users", nil),
				"b": vertex("purchases:57", "purchases", data{"state": "IL"}),
			}),
		},
		{
			Q.V("users:1").As("a").Fields().Out().As("b").Fields().Select("a", "b"),
			pickSelection(map[string]interface{}{
				"a": getVertex("users:1"),
				"b": getVertex("purchases:57"),
			}),
		},
		{
			Q.V().Match(
				Q.HasLabel("products"),
				Q.Has(gripql.Eq("price", 499.99)),
			),
			pick("products:6"),
		},
		{
			Q.V().Match(
				Q.As("a").HasLabel("products").As("b"),
				Q.As("b").Has(gripql.Eq("price", 499.99)).As("c"),
			).Select("c"),
			pick("products:6"),
		},
		{
			Q.V("users:1").As("a").Out().As("b").
				Render(map[string]interface{}{"user_id": "$a._gid", "purchase_id": "$b._gid", "purchaser": "$b.name"}),
			render(map[string]interface{}{"user_id": "users:1", "purchase_id": "purchases:57", "purchaser": "Letitia Sprau"}),
		},
	}

	for _, desc := range tests {
		desc := desc
		name := cleanName(dbname + "_" + desc.query.String())

		t.Run(name, func(t *testing.T) {
			compiledPipeline, err := db.Compiler().Compile(desc.query.Statements)
			if err != nil {
				t.Fatal(err)
			}
			workdir := "./test.workdir." + util.RandomString(6)
			defer os.RemoveAll(workdir)
			res := pipeline.Run(context.Background(), compiledPipeline, workdir)
			desc.expected(t, res)
		})
	}
}

func vertex(gid, label string, d data) *gripql.Vertex {
	ds, _ := structpb.NewStruct(d)
	return &gripql.Vertex{
		Gid:   gid,
		Label: label,
		Data:  ds,
	}
}

func edge(gid interface{}, from, to string, label string, d data) *gripql.Edge {
	ds, _ := structpb.NewStruct(d)
	return &gripql.Edge{
		Gid:   fmt.Sprintf("%v", gid),
		From:  from,
		To:    to,
		Label: label,
		Data:  ds,
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
	vs, _ := structpb.NewValue(v)
	expect := []*gripql.QueryResult{
		{
			Result: &gripql.QueryResult_Render{
				Render: vs,
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
