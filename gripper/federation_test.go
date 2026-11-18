package gripper_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripper/node"
	"github.com/bmeg/grip/gripper/node/nodetest"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const swapiDir = "../conformance/graphs/"

func readLines(t testing.TB, path string, fn func(map[string]interface{})) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		m := map[string]interface{}{}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		fn(m)
	}
}

type swapiData struct {
	vertices []*gdbi.Vertex
	edges    []*gdbi.Edge
}

func loadSwapi(t testing.TB) *swapiData {
	t.Helper()
	d := &swapiData{}
	readLines(t, swapiDir+"swapi.vertices", func(m map[string]interface{}) {
		v := &gdbi.Vertex{ID: m["_id"].(string), Label: m["_label"].(string), Loaded: true}
		delete(m, "_id")
		delete(m, "_label")
		v.Data = m
		d.vertices = append(d.vertices, v)
	})
	readLines(t, swapiDir+"swapi.edges", func(m map[string]interface{}) {
		e := &gdbi.Edge{ID: m["_id"].(string), Label: m["_label"].(string),
			From: m["_from"].(string), To: m["_to"].(string), Loaded: true}
		for _, k := range []string{"_id", "_label", "_from", "_to"} {
			delete(m, k)
		}
		e.Data = m
		d.edges = append(d.edges, e)
	})
	return d
}

// counting wraps a client and counts the Traverse streams opened through it.
type counting struct {
	gripper.GRIPSourceClient
	streams *atomic.Int64
}

func (c counting) Traverse(ctx context.Context, opts ...grpc.CallOption) (gripper.GRIPSource_TraverseClient, error) {
	c.streams.Add(1)
	return c.GRIPSourceClient.Traverse(ctx, opts...)
}

var traverseStreams atomic.Int64

// monolithic loads everything into one graph.
func monolithic(t testing.TB, d *swapiData) gdbi.GraphInterface {
	g := nodetest.NewGraph(t, "mono")
	if err := g.AddVertex(d.vertices); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(d.edges); err != nil {
		t.Fatal(err)
	}
	return g
}

// federated starts one node per ID prefix; edges are stored with their source vertex.
func federated(t testing.TB, d *swapiData) gdbi.GraphInterface {
	prefixes := map[string]bool{}
	for _, v := range d.vertices {
		prefixes[node.CuriePrefix(v.ID)] = true
	}
	reg := gripper.NewRegistry()
	for p := range prefixes {
		g := nodetest.NewGraph(t, strings.TrimSuffix(p, ":"))
		vs := []*gdbi.Vertex{}
		for _, v := range d.vertices {
			if strings.HasPrefix(v.ID, p) {
				vs = append(vs, v)
			}
		}
		es := []*gdbi.Edge{}
		for _, e := range d.edges {
			if strings.HasPrefix(e.From, p) {
				es = append(es, e)
			}
		}
		if err := g.AddVertex(vs); err != nil {
			t.Fatal(err)
		}
		if len(es) > 0 {
			if err := g.AddEdge(es); err != nil {
				t.Fatal(err)
			}
		}
		links, err := node.ScanEdgeLinks(context.Background(), g, p)
		if err != nil {
			t.Fatal(err)
		}
		srv, err := node.NewServer(g, node.Options{Name: p, Prefix: p, EdgeLinks: links})
		if err != nil {
			t.Fatal(err)
		}
		if err := reg.Add(p, counting{nodetest.Serve(t, srv), &traverseStreams}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := gripper.NewFederatedGDB("swapi", reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	g, err := db.Graph("swapi")
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func runQuery(t testing.TB, g gdbi.GraphInterface, q *gripql.Query) []string {
	t.Helper()
	pipe, err := g.Compiler().Compile(q.Statements, nil)
	if err != nil {
		t.Fatalf("compile %s: %v", q, err)
	}
	out := []string{}
	for r := range pipeline.Run(context.Background(), pipe, t.TempDir()) {
		b, err := protojson.MarshalOptions{}.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		// protojson output is deliberately unstable in whitespace; normalize through json
		var v interface{}
		json.Unmarshal(b, &v)
		n, _ := json.Marshal(v)
		out = append(out, string(n))
	}
	sort.Strings(out)
	return out
}

var corpus = map[string]*gripql.Query{
	"vertices by label":  gripql.NewQuery().V().HasLabel("Character"),
	"vertex by id":       gripql.NewQuery().V("Character:1"),
	"missing vertex":     gripql.NewQuery().V("Character:99999"),
	"has":                gripql.NewQuery().V().HasLabel("Character").Has(gripql.Eq("name", "Luke Skywalker")),
	"out all":            gripql.NewQuery().V("Character:1").Out(),
	"out label":          gripql.NewQuery().V("Character:1").Out("homeworld"),
	"out out":            gripql.NewQuery().V("Film:1").Out("characters").Out("homeworld"),
	"in":                 gripql.NewQuery().V("Planet:1").In("homeworld"),
	"in all":             gripql.NewQuery().V("Planet:1").In(),
	"in label chain":     gripql.NewQuery().V().HasLabel("Planet").In("homeworld").Out("films"),
	"both":               gripql.NewQuery().V("Character:1").Both(),
	"outE":               gripql.NewQuery().V("Character:1").OutE(),
	"inE":                gripql.NewQuery().V("Planet:1").InE("homeworld"),
	"bothE":              gripql.NewQuery().V("Character:1").BothE("films"),
	"outE then out":      gripql.NewQuery().V("Character:1").OutE("homeworld").Out(),
	"inE then in":        gripql.NewQuery().V("Planet:1").InE("homeworld").In(),
	"edge has label":     gripql.NewQuery().V().HasLabel("Film").OutE().HasLabel("characters"),
	"outNull":            gripql.NewQuery().V().HasLabel("Character").OutNull("starships"),
	"inNull":             gripql.NewQuery().V().HasLabel("Planet").InNull("homeworld"),
	"mark and render":    gripql.NewQuery().V().HasLabel("Character").As("a").Out("homeworld").As("b").Render([]string{"$a._id", "$b._id", "$b._label"}),
	"select":             gripql.NewQuery().V("Film:1").As("f").Out("characters").As("c").Select("f"),
	"count":              gripql.NewQuery().V().HasLabel("Character").Out("films").Count(),
	"distinct":           gripql.NewQuery().V().HasLabel("Character").Out("species").Distinct(),
	"has after move":     gripql.NewQuery().V("Film:1").Out("characters").Has(gripql.Eq("eye_color", "blue")),
	"fields":             gripql.NewQuery().V("Character:1").Out("homeworld").Fields("name"),
	"two hop with marks": gripql.NewQuery().V("Character:1").As("c").Out("films").As("f").Out("planets").Render([]string{"$c._id", "$f._id", "_id"}),
}

func repeatQuery() *gripql.Query {
	q := gripql.NewQuery().V("Character:1")
	q.Statements = append(q.Statements,
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Set{Set: &gripql.Set{Key: "count", Value: structpb.NewNumberValue(0)}}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_As{As: "start"}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Mark{Mark: "a"}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Out{Out: protoutil.NewListFromStrings(nil)}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Increment{Increment: &gripql.Increment{Key: "$start.count", Value: 1}}},
	)
	q = q.Has(gripql.Lt("$start.count", 2))
	q.Statements = append(q.Statements,
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Jump{Jump: &gripql.Jump{Mark: "a", Emit: true}}})
	return q
}

func TestFederatedMatchesMonolithic(t *testing.T) {
	d := loadSwapi(t)
	mono := monolithic(t, d)
	fed := federated(t, d)
	queries := map[string]*gripql.Query{"repeat with jump": repeatQuery()}
	for k, v := range corpus {
		queries[k] = v
	}
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			want := runQuery(t, mono, q)
			got := runQuery(t, fed, q)
			if strings.Join(want, "\n") != strings.Join(got, "\n") {
				t.Errorf("%s\nmonolithic (%d results):\n%v\nfederated (%d results):\n%v", q, len(want), trunc(want), len(got), trunc(got))
			}
			if len(want) == 0 && name != "missing vertex" {
				t.Logf("note: query returns nothing")
			}
		})
	}
}

func trunc(s []string) []string {
	if len(s) > 6 {
		return s[:6]
	}
	return s
}

// A pushed down traversal opens a few streams per source no matter how many
// travelers it moves; the single-step baseline opens streams per lookup batch.
func TestPushdownStreamCount(t *testing.T) {
	d := loadSwapi(t)
	fed := federated(t, d)
	q := gripql.NewQuery().V().HasLabel("Character").Out("films").Out("planets")

	traverseStreams.Store(0)
	rows := runQuery(t, fed, q)
	if len(rows) == 0 {
		t.Fatal("no results")
	}
	// 6 sources: seeds + handoff resolution of the segment
	t.Logf("streams opened: %d for %d results", traverseStreams.Load(), len(rows))
	if n := traverseStreams.Load(); n > 12 {
		t.Errorf("segment used %d streams for %d results", n, len(rows))
	}
}

// A downstream limit must stop an otherwise endless jump loop that runs through a segment.
func TestInfiniteLoopWithLimit(t *testing.T) {
	d := loadSwapi(t)
	fed := federated(t, d)
	q := gripql.NewQuery().V("Character:1")
	q.Statements = append(q.Statements,
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Mark{Mark: "a"}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Out{Out: protoutil.NewListFromStrings(nil)}},
		&gripql.GraphStatement{Statement: &gripql.GraphStatement_Jump{Jump: &gripql.Jump{Mark: "a", Emit: true}}},
	)
	q = q.Limit(100)
	done := make(chan []string, 1)
	go func() { done <- runQuery(t, fed, q) }()
	select {
	case rows := <-done:
		if len(rows) != 100 {
			t.Errorf("got %d rows, want 100", len(rows))
		}
	case <-time.After(60 * time.Second):
		t.Fatal("jump loop with limit did not terminate")
	}
}

func TestLimitStress(t *testing.T) {
	d := loadSwapi(t)
	fed := federated(t, d)
	queries := []*gripql.Query{
		gripql.NewQuery().V().Limit(3),
		gripql.NewQuery().V().OutE().Limit(3),
		gripql.NewQuery().V().HasLabel("Character").Out().Limit(5),
		gripql.NewQuery().V().Out().Out().Limit(3),
	}
	for i := 0; i < 100; i++ {
		for _, q := range queries {
			rows := runQuery(t, fed, q)
			if len(rows) == 0 {
				t.Fatalf("%s returned nothing", q)
			}
		}
	}
}
