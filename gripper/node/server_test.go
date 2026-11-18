package node_test

import (
	"context"
	"sort"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripper/node"
	"github.com/bmeg/grip/gripper/node/nodetest"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func vertex(id, label string, n int) *gdbi.Vertex {
	return &gdbi.Vertex{ID: id, Label: label, Data: map[string]interface{}{"n": float64(n)}, Loaded: true}
}

func edge(from, label, to string) *gdbi.Edge {
	return &gdbi.Edge{ID: from + "-" + label + "-" + to, From: from, To: to, Label: label, Data: map[string]interface{}{}, Loaded: true}
}

// startNodes creates node A (vertices A:1, A:2 and their edges, two of which point at B)
// and node B (vertices B:1, B:2).
func startNodes(t *testing.T) (a, b gripper.GRIPSourceClient) {
	t.Helper()
	ga := nodetest.NewGraph(t, "a")
	if err := ga.AddVertex([]*gdbi.Vertex{vertex("A:1", "A", 1), vertex("A:2", "A", 2)}); err != nil {
		t.Fatal(err)
	}
	if err := ga.AddEdge([]*gdbi.Edge{
		edge("A:1", "l", "B:1"), edge("A:1", "l", "B:2"),
		edge("A:2", "l", "A:1"), edge("A:2", "l", "B:9"),
	}); err != nil {
		t.Fatal(err)
	}
	gb := nodetest.NewGraph(t, "b")
	if err := gb.AddVertex([]*gdbi.Vertex{vertex("B:1", "B", 10), vertex("B:2", "B", 20)}); err != nil {
		t.Fatal(err)
	}
	sa, err := node.NewServer(ga, node.Options{Name: "a", Prefix: "A:"})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := node.NewServer(gb, node.Options{Name: "b", Prefix: "B:"})
	if err != nil {
		t.Fatal(err)
	}
	return nodetest.Serve(t, sa), nodetest.Serve(t, sb)
}

// traverse sends one batch followed by a flush and returns everything received before the ack.
func traverse(t *testing.T, c gripper.GRIPSourceClient, steps []*gripql.GraphStatement, entries []uint32, in ...*gripper.TravelerIn) []*gripper.TravelerOut {
	t.Helper()
	out, err := tryTraverse(t, c, steps, entries, in...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func tryTraverse(t *testing.T, c gripper.GRIPSourceClient, steps []*gripql.GraphStatement, entries []uint32, in ...*gripper.TravelerIn) ([]*gripper.TravelerOut, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := c.Traverse(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Start{
		Start: &gripper.Start{Steps: steps, EntryCursors: entries, LoadFinal: true}}}); err != nil {
		return nil, err
	}
	stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Batch{Batch: &gripper.TravelerBatch{Travelers: in}}})
	stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Flush{Flush: &gripper.Flush{Id: 7}}})
	out := []*gripper.TravelerOut{}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return out, err
		}
		switch m := msg.Msg.(type) {
		case *gripper.TraverseResponse_Results:
			out = append(out, m.Results.Results...)
		case *gripper.TraverseResponse_Ack:
			if m.Ack.Id != 7 {
				t.Fatalf("unexpected ack %d", m.Ack.Id)
			}
			stream.CloseSend()
			return out, nil
		case *gripper.TraverseResponse_Error:
			t.Fatalf("node error: %s", m.Error.Message)
		}
	}
}

func ids(res []*gripper.TravelerOut) []string {
	out := []string{}
	for _, r := range res {
		out = append(out, r.Current.Id)
	}
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func seed(id uint64) *gripper.TravelerIn { return &gripper.TravelerIn{RequestId: id} }

func TestSeedHasLabel(t *testing.T) {
	a, b := startNodes(t)
	steps := gripql.NewQuery().V().HasLabel("A").Statements
	res := traverse(t, a, steps, []uint32{0}, seed(1))
	if got := ids(res); !equal(got, []string{"A:1", "A:2"}) {
		t.Errorf("node A: %v", got)
	}
	for _, r := range res {
		if r.RequestId != 1 || r.Cursor != 2 || !r.Current.Loaded || r.Current.Data == nil {
			t.Errorf("bad result %v", r)
		}
	}
	if res := traverse(t, b, steps, []uint32{0}, seed(2)); len(res) != 0 {
		t.Errorf("node B returned %d results for label A", len(res))
	}
}

func TestHandoffAndResolve(t *testing.T) {
	a, b := startNodes(t)
	// V().hasLabel(A).as(src).out(l).hasLabel(B)
	steps := gripql.NewQuery().V().HasLabel("A").As("src").Out("l").HasLabel("B").Statements

	res := traverse(t, a, steps, []uint32{0, 4}, seed(1))
	local, handoff := []*gripper.TravelerOut{}, []*gripper.TravelerOut{}
	for _, r := range res {
		if int(r.Cursor) == len(steps) {
			local = append(local, r)
		} else {
			handoff = append(handoff, r)
		}
	}
	// A:2 -l-> A:1 is local but fails hasLabel(B); the three foreign edges are handed off
	if len(local) != 0 {
		t.Errorf("unexpected local results: %v", ids(local))
	}
	if got := ids(handoff); !equal(got, []string{"B:1", "B:2", "B:9"}) {
		t.Fatalf("handoffs: %v", got)
	}
	for _, h := range handoff {
		if h.Cursor != 4 || h.Current.Loaded {
			t.Errorf("handoff must be a bare reference at the step after the move: %v", h)
		}
		if h.Marks["src"] == nil || len(h.Path) != 2 {
			t.Errorf("segment delta missing marks/path: %v", h)
		}
	}

	// node B resolves the references and finishes the segment; B:9 does not exist
	in := []*gripper.TravelerIn{}
	for i, h := range handoff {
		in = append(in, &gripper.TravelerIn{RequestId: uint64(100 + i), Cursor: h.Cursor, Current: h.Current, NeedsResolve: true})
	}
	res = traverse(t, b, steps, []uint32{0, 4}, in...)
	if got := ids(res); !equal(got, []string{"B:1", "B:2"}) {
		t.Errorf("resolved: %v", got)
	}
	for _, r := range res {
		if r.Cursor != 5 || !r.Current.Loaded || r.Current.Label != "B" {
			t.Errorf("bad resolved result %v", r)
		}
	}
}

func TestLocalMoveAndEntryAtCursor(t *testing.T) {
	a, _ := startNodes(t)
	steps := gripql.NewQuery().V().HasLabel("A").Out("l").HasLabel("A").Statements
	// entering at cursor 3 with a loaded element runs the remaining steps only
	cur := &gripper.Element{Id: "A:1", Label: "A", Loaded: true}
	res := traverse(t, a, steps, []uint32{0, 3}, &gripper.TravelerIn{RequestId: 5, Cursor: 3, Current: cur})
	if len(res) != 1 || res[0].Current.Id != "A:1" || res[0].Cursor != 4 || len(res[0].Path) != 0 {
		t.Errorf("entry at cursor 3: %v", res)
	}
	// full segment from the seed: A:2 -l-> A:1 is the only local A to A edge
	res = traverse(t, a, steps, []uint32{0}, seed(6))
	local := []*gripper.TravelerOut{}
	for _, r := range res {
		if int(r.Cursor) == len(steps) {
			local = append(local, r)
		}
	}
	if got := ids(local); !equal(got, []string{"A:1"}) {
		t.Errorf("local results %v", got)
	}
}

func TestForeignProbe(t *testing.T) {
	a, _ := startNodes(t)
	// inE at cursor 1 for a vertex node A does not own: reports edges stored at A that point at B:1
	steps := gripql.NewQuery().V().InE("l").Statements
	probe := &gripper.TravelerIn{RequestId: 3, Cursor: 1, Current: &gripper.Element{Id: "B:1"}, ForeignProbe: true}
	res := traverse(t, a, steps, []uint32{1}, probe)
	if got := ids(res); !equal(got, []string{"A:1-l-B:1"}) {
		t.Errorf("probe results %v", got)
	}
	if res[0].Current.To != "B:1" || res[0].RequestId != 3 {
		t.Errorf("bad probe result %v", res[0])
	}
}

func TestNullStepsRejected(t *testing.T) {
	a, _ := startNodes(t)
	steps := gripql.NewQuery().V().HasID("A:1").OutNull("l").Statements
	if _, err := tryTraverse(t, a, steps, []uint32{0}, seed(1)); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestMultipleBatchesAndFlushOrdering(t *testing.T) {
	a, _ := startNodes(t)
	steps := gripql.NewQuery().V().HasLabel("A").Statements
	ctx := context.Background()
	stream, err := a.Traverse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Start{Start: &gripper.Start{Steps: steps, EntryCursors: []uint32{0}}}})
	const batches = 20
	for i := 0; i < batches; i++ {
		stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Batch{Batch: &gripper.TravelerBatch{
			Travelers: []*gripper.TravelerIn{seed(uint64(i))}}}})
		stream.Send(&gripper.TraverseRequest{Msg: &gripper.TraverseRequest_Flush{Flush: &gripper.Flush{Id: uint64(i)}}})
	}
	stream.CloseSend()
	counts := map[uint64]int{}
	acks := 0
	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		switch m := msg.Msg.(type) {
		case *gripper.TraverseResponse_Results:
			for _, r := range m.Results.Results {
				counts[r.RequestId]++
			}
		case *gripper.TraverseResponse_Ack:
			// every result of batches 0..ack must have been received already
			for i := uint64(0); i <= m.Ack.Id; i++ {
				if counts[i] != 2 {
					t.Fatalf("ack %d arrived with %d of 2 results for request %d", m.Ack.Id, counts[i], i)
				}
			}
			acks++
		}
	}
	if acks != batches {
		t.Errorf("got %d acks", acks)
	}
}

func TestInvalidSegments(t *testing.T) {
	a, _ := startNodes(t)
	cases := map[string][]*gripql.GraphStatement{
		"limit":        gripql.NewQuery().V().Limit(1).Statements,
		"count":        gripql.NewQuery().V().Count().Statements,
		"second V":     append(gripql.NewQuery().V().Statements, gripql.NewQuery().V().Statements...),
		"unknown mark": gripql.NewQuery().V().Has(gripql.Eq("$x.n", 1)).Statements,
	}
	for name, steps := range cases {
		_, err := tryTraverse(t, a, steps, []uint32{0}, seed(1))
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
	// unsupported entry cursor
	_, err := tryTraverse(t, a, gripql.NewQuery().V().HasLabel("A").Statements, []uint32{0},
		&gripper.TravelerIn{RequestId: 1, Cursor: 1, Current: &gripper.Element{Id: "A:1"}})
	if err == nil {
		t.Error("traveler at undeclared cursor accepted")
	}
}

func TestGetInfo(t *testing.T) {
	a, _ := startNodes(t)
	info, err := a.GetInfo(context.Background(), &gripper.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Prefix != "A:" || info.ProtocolVersion != gripper.ProtocolVersion {
		t.Errorf("info %v", info)
	}
	if len(info.Schema.Vertices) != 1 || info.Schema.Vertices[0].Label != "A" {
		t.Errorf("schema %v", info.Schema)
	}
}
