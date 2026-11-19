package table_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper/node"
	griptable "github.com/bmeg/grip/table"
)

type graphFakeTable struct {
	collections map[string]griptable.Collection
	rows        map[string][]*griptable.Row
}

func (f *graphFakeTable) Capabilities() griptable.Capabilities {
	return griptable.Capabilities{Readable: true}
}

func (f *graphFakeTable) ListCollections(context.Context) ([]griptable.Collection, error) {
	out := make([]griptable.Collection, 0, len(f.collections))
	for _, collection := range f.collections {
		out = append(out, collection)
	}
	return out, nil
}

func (f *graphFakeTable) GetCollection(_ context.Context, name string) (griptable.Collection, error) {
	collection, ok := f.collections[name]
	if !ok {
		return griptable.Collection{}, griptable.ErrCollectionNotFound
	}
	return collection, nil
}

func (f *graphFakeTable) Scan(ctx context.Context, collection string, _ griptable.ScanOptions) (griptable.RowStream, error) {
	if _, ok := f.collections[collection]; !ok {
		return nil, griptable.ErrCollectionNotFound
	}
	out := make(chan griptable.RowResult)
	go func() {
		defer close(out)
		for _, row := range f.rows[collection] {
			select {
			case <-ctx.Done():
				out <- griptable.RowResult{Err: ctx.Err()}
				return
			case out <- griptable.RowResult{Row: row}:
			}
		}
	}()
	return out, nil
}

func (f *graphFakeTable) LookupByID(_ context.Context, collection, id string) (*griptable.Row, error) {
	for _, row := range f.rows[collection] {
		if row.ID == id {
			return row, nil
		}
	}
	return nil, griptable.ErrRowNotFound
}

func (f *graphFakeTable) LookupByField(ctx context.Context, collection, field string, value interface{}) (griptable.RowStream, error) {
	out, err := f.Scan(ctx, collection, griptable.ScanOptions{})
	if err != nil {
		return nil, err
	}
	filtered := make(chan griptable.RowResult)
	go func() {
		defer close(filtered)
		for result := range out {
			if result.Err != nil || result.Row.Data[field] == value {
				filtered <- result
			}
		}
	}()
	return filtered, nil
}

func testGraph(t *testing.T) *griptable.Graph {
	t.Helper()
	driver := &graphFakeTable{
		collections: map[string]griptable.Collection{
			"people": {
				Name: "people", PrimaryKey: "id",
				Fields: []griptable.Field{{Name: "id", Indexed: true}, {Name: "name"}},
			},
			"planet": {
				Name: "planet", PrimaryKey: "id",
				Fields: []griptable.Field{{Name: "id", Indexed: true}, {Name: "name"}},
			},
			"friends": {
				Name: "friends", PrimaryKey: "id",
				Fields: []griptable.Field{{Name: "from"}, {Name: "to"}},
			},
		},
		rows: map[string][]*griptable.Row{
			"people": {
				{ID: "1", Data: map[string]interface{}{"id": "1", "name": "Luke"}},
				{ID: "2", Data: map[string]interface{}{"id": "2", "name": "Leia"}},
			},
			"planet": {
				{ID: "1", Data: map[string]interface{}{"id": "1", "name": "Tatooine"}},
			},
			"friends": {
				{ID: "1-friends-2", Data: map[string]interface{}{"from": "1", "to": "2"}},
			},
		},
	}
	mapping := griptable.Mapping{
		Vertices: []griptable.VertexTable{
			{Collection: "people", Label: "Person", Prefix: "Person:", ID: "id"},
			{Collection: "planet", Label: "Planet", Prefix: "Planet:", ID: "id"},
		},
		Edges: []griptable.EdgeTable{{
			Collection: "friends",
			Label:      "friends",
			From:       griptable.Endpoint{Field: "from", Prefix: "Person:"},
			To:         griptable.Endpoint{Field: "to", Prefix: "Person:"},
		}},
	}
	graph, err := griptable.NewGraph(context.Background(), driver, mapping)
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func TestTableGraphPointAndListLookups(t *testing.T) {
	graph := testGraph(t)

	vertex := graph.GetVertex("Person:1", true)
	if vertex == nil || vertex.ID != "Person:1" || vertex.Label != "Person" ||
		vertex.Data["name"] != "Luke" {
		t.Fatalf("GetVertex() = %#v", vertex)
	}
	if bare := graph.GetVertex("Person:1", false); bare == nil || bare.Loaded {
		t.Fatalf("GetVertex(load=false) = %#v", bare)
	}

	edge := graph.GetEdge("Person:1-friends-Person:2", true)
	if edge == nil || edge.From != "Person:1" || edge.To != "Person:2" {
		t.Fatalf("GetEdge() = %#v", edge)
	}

	vertexLabels, err := graph.ListVertexLabels()
	if err != nil || len(vertexLabels) != 2 {
		t.Fatalf("ListVertexLabels() = %v, %v", vertexLabels, err)
	}
	edgeLabels, err := graph.ListEdgeLabels()
	if err != nil || len(edgeLabels) != 1 || edgeLabels[0] != "friends" {
		t.Fatalf("ListEdgeLabels() = %v, %v", edgeLabels, err)
	}

	vertices := make([]string, 0)
	for vertex := range graph.GetVertexList(context.Background(), false) {
		vertices = append(vertices, vertex.ID)
	}
	if len(vertices) != 3 {
		t.Fatalf("GetVertexList returned %v", vertices)
	}
}

func TestTableGraphAdjacencyChannels(t *testing.T) {
	graph := testGraph(t)

	req := make(chan gdbi.ElementLookup, 1)
	req <- gdbi.ElementLookup{ID: "Person:1"}
	close(req)

	out := graph.GetOutChannel(context.Background(), req, true, false, nil)
	found := make([]gdbi.ElementLookup, 0)
	for lookup := range out {
		found = append(found, lookup)
	}
	if len(found) != 1 || found[0].Vertex == nil || found[0].Vertex.Get().ID != "Person:2" {
		t.Fatalf("GetOutChannel() = %#v", found)
	}

	inReq := make(chan gdbi.ElementLookup, 1)
	inReq <- gdbi.ElementLookup{ID: "Person:2"}
	close(inReq)
	inFound := make([]gdbi.ElementLookup, 0)
	for lookup := range graph.GetInChannel(context.Background(), inReq, true, false, nil) {
		inFound = append(inFound, lookup)
	}
	if len(inFound) != 1 || inFound[0].Vertex.Get().ID != "Person:1" {
		t.Fatalf("GetInChannel() = %#v", inFound)
	}

	edgeReq := make(chan gdbi.ElementLookup, 1)
	edgeReq <- gdbi.ElementLookup{ID: "Person:1"}
	close(edgeReq)
	edges := graph.GetOutEdgeChannel(context.Background(), edgeReq, true, false, nil)
	edgeLookup := <-edges
	if edgeLookup.Edge == nil || edgeLookup.Edge.Get().ID != "Person:1-friends-Person:2" {
		t.Fatalf("GetOutEdgeChannel() = %#v", edgeLookup)
	}
}

func TestTableGraphNullAndReadOnlyMutation(t *testing.T) {
	graph := testGraph(t)

	req := make(chan gdbi.ElementLookup, 1)
	req <- gdbi.ElementLookup{ID: "Planet:1"}
	close(req)
	out := graph.GetOutChannel(context.Background(), req, true, true, nil)
	lookup := <-out
	if lookup.Vertex != nil {
		t.Fatalf("GetOutChannel emitNull produced %#v", lookup)
	}

	if err := graph.AddVertex([]*gdbi.Vertex{{ID: "Person:3", Label: "Person"}}); !errors.Is(err, griptable.ErrUnsupported) {
		t.Fatalf("AddVertex() = %v", err)
	}
	if err := graph.DelEdge("Person:1-friends-Person:2"); !errors.Is(err, griptable.ErrUnsupported) {
		t.Fatalf("DelEdge() = %v", err)
	}
}

func TestTableGraphValidation(t *testing.T) {
	driver := &graphFakeTable{
		collections: map[string]griptable.Collection{},
		rows:        map[string][]*griptable.Row{},
	}
	_, err := griptable.NewGraph(context.Background(), driver, griptable.Mapping{})
	if !errors.Is(err, griptable.ErrInvalidRequest) {
		t.Fatalf("NewGraph(empty mapping) = %v", err)
	}
}

func TestTableGraphSignalsAndLabelScan(t *testing.T) {
	graph := testGraph(t)

	req := make(chan gdbi.ElementLookup, 1)
	req <- gdbi.ElementLookup{
		ID:  "Person:1",
		Ref: &gdbi.BaseTraveler{Signal: &gdbi.Signal{Dest: "test", ID: 7}},
	}
	close(req)
	out := graph.GetVertexChannel(context.Background(), req, true)
	lookup := <-out
	if !lookup.IsSignal() {
		t.Fatalf("signal lookup was not forwarded: %#v", lookup)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	count := 0
	for range graph.VertexLabelScan(ctx, "Person") {
		count++
	}
	if count != 0 {
		t.Fatalf("cancelled label scan returned %d vertices", count)
	}
}

func TestTableGraphAsResourceNode(t *testing.T) {
	graph := testGraph(t)
	server, err := node.NewServer(graph, node.Options{
		Name: "table-node", Prefix: "Person:",
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := server.GetInfo(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "table-node" || info.Prefix != "Person:" {
		t.Fatalf("GetInfo() = %#v", info)
	}
}
