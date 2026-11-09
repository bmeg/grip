package gripper

import (
	"context"
	"fmt"
	"sort"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// FederatedGDB serves one federated graph built from the sources in a Registry.
type FederatedGDB struct {
	name  string
	graph *FederatedGraph
}

// NewFederatedGDB creates the graph database for a federation. sources lists the
// names of the registry sources to use; empty means every source in the registry.
func NewFederatedGDB(name string, reg *Registry, sources []string) (gdbi.GraphDB, error) {
	if name == "" {
		return nil, fmt.Errorf("federated graph needs a name")
	}
	var filter map[string]bool
	if len(sources) > 0 {
		filter = map[string]bool{}
		for _, s := range sources {
			if reg.Source(s) == nil {
				return nil, fmt.Errorf("federated graph %s: unknown source %s", name, s)
			}
			filter[s] = true
		}
	}
	log.Infof("Starting GRIPPER federation %s", name)
	return &FederatedGDB{name: name, graph: &FederatedGraph{reg: reg, filter: filter}}, nil
}

func (db *FederatedGDB) AddGraph(string) error {
	return fmt.Errorf("AddGraph not supported by a federated graph")
}

func (db *FederatedGDB) DeleteGraph(string) error {
	return fmt.Errorf("DeleteGraph not supported by a federated graph")
}

func (db *FederatedGDB) ListGraphs() []string { return []string{db.name} }

func (db *FederatedGDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	if graphID != db.name {
		return nil, fmt.Errorf("graph %s not found", graphID)
	}
	return db.graph, nil
}

func (db *FederatedGDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
	if graphID != db.name {
		return nil, fmt.Errorf("graph %s not found", graphID)
	}
	return db.graph.Schema(graphID), nil
}

func (db *FederatedGDB) Close() error { return nil }

// FederatedGraph is a read only gdbi.GraphInterface over the sources of a federation.
// Every operation is a single-step traversal run on the sources.
type FederatedGraph struct {
	reg    *Registry
	filter map[string]bool
}

func (g *FederatedGraph) member(s *Source) bool {
	return s != nil && (g.filter == nil || g.filter[s.Name])
}

func (g *FederatedGraph) owner(id string) *Source {
	if s := g.reg.Owner(id); g.member(s) {
		return s
	}
	return nil
}

func (g *FederatedGraph) inbound(prefix string, labels []string) []*Source {
	out := []*Source{}
	for _, s := range g.reg.InboundSources(prefix, labels) {
		if g.member(s) {
			out = append(out, s)
		}
	}
	return out
}

func (g *FederatedGraph) sourcesFor(labels, ids []string) []*Source {
	out := []*Source{}
	for _, s := range g.reg.SourcesFor(labels, ids) {
		if g.member(s) {
			out = append(out, s)
		}
	}
	return out
}

func (g *FederatedGraph) members() []*Source {
	return g.sourcesFor(nil, nil)
}

// Schema merges the schemas of the member sources.
func (g *FederatedGraph) Schema(name string) *gripql.Graph {
	out := &gripql.Graph{Graph: name}
	seenV := map[string]bool{}
	seenE := map[[3]string]bool{}
	for _, s := range g.members() {
		for _, v := range s.Info.GetSchema().GetVertices() {
			if !seenV[v.Label] {
				seenV[v.Label] = true
				out.Vertices = append(out.Vertices, v)
			}
		}
		for _, e := range s.Info.GetSchema().GetEdges() {
			k := [3]string{e.Label, e.From, e.To}
			if !seenE[k] {
				seenE[k] = true
				out.Edges = append(out.Edges, e)
			}
		}
	}
	return out
}

func (g *FederatedGraph) GetTimestamp() string { return "NA" }

func (g *FederatedGraph) GetVertex(key string, load bool) *gdbi.Vertex {
	req := make(chan gdbi.ElementLookup, 1)
	req <- gdbi.ElementLookup{ID: key}
	close(req)
	var out *gdbi.Vertex
	for r := range g.GetVertexChannel(context.Background(), req, load) {
		if r.Vertex != nil {
			out = r.Vertex.Get()
		}
	}
	return out
}

// GetEdge finds an edge by ID by scanning the edges of the source that owns it.
func (g *FederatedGraph) GetEdge(key string, load bool) *gdbi.Edge {
	src := g.owner(key)
	if src == nil {
		return nil
	}
	steps := gripql.NewQuery().V().OutE().HasID(key).Statements
	res, err := g.scan(context.Background(), src, steps)
	if err != nil {
		log.Errorf("GetEdge: %s", err)
		return nil
	}
	var out *gdbi.Edge
	for r := range res {
		out = ProtoToElement(r.Current)
	}
	return out
}

func (g *FederatedGraph) AddVertex([]*gdbi.Vertex) error          { return errReadOnly }
func (g *FederatedGraph) AddEdge([]*gdbi.Edge) error              { return errReadOnly }
func (g *FederatedGraph) BulkAdd(<-chan *gdbi.GraphElement) error { return errReadOnly }
func (g *FederatedGraph) BulkDel(*gdbi.DeleteData) error          { return errReadOnly }
func (g *FederatedGraph) DelVertex(string) error                  { return errReadOnly }
func (g *FederatedGraph) DelEdge(string) error                    { return errReadOnly }
func (g *FederatedGraph) AddVertexIndex(string, string) error     { return errReadOnly }
func (g *FederatedGraph) DeleteVertexIndex(string, string) error  { return errReadOnly }

func (g *FederatedGraph) GetVertexIndexList() <-chan *gripql.IndexID {
	out := make(chan *gripql.IndexID)
	close(out)
	return out
}

func (g *FederatedGraph) ListVertexLabels() ([]string, error) {
	return labelList(g.Schema("").Vertices), nil
}

func (g *FederatedGraph) ListEdgeLabels() ([]string, error) {
	return labelList(g.Schema("").Edges), nil
}

func labelList[T interface{ GetLabel() string }](items []T) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, i := range items {
		if l := i.GetLabel(); !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}

// VertexLabelScan streams the IDs of all vertices with the label.
func (g *FederatedGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	steps := gripql.NewQuery().V().HasLabel(label).Statements
	go func() {
		defer close(out)
		for _, src := range g.sourcesFor([]string{label}, nil) {
			res, err := g.scan(ctx, src, steps)
			if err != nil {
				log.Errorf("VertexLabelScan: %s", err)
				return
			}
			for r := range res {
				select {
				case out <- r.Current.Id:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// GetVertexList streams all vertices of all sources.
func (g *FederatedGraph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	out := make(chan *gdbi.Vertex, 100)
	steps := []*gripql.GraphStatement{vStatement()}
	go func() {
		defer close(out)
		for _, src := range g.members() {
			res, err := g.scan(ctx, src, steps)
			if err != nil {
				log.Errorf("GetVertexList: %s", err)
				return
			}
			for r := range res {
				select {
				case out <- ProtoToElement(r.Current):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func moveSteps(move *gripql.GraphStatement) []*gripql.GraphStatement {
	return []*gripql.GraphStatement{vStatement(), move}
}

func (g *FederatedGraph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	return g.lookup(ctx, req, lookupSpec{steps: []*gripql.GraphStatement{vStatement()}, mode: lookupVertices, resolve: true})
}

func (g *FederatedGraph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	steps := moveSteps(gripql.NewQuery().Out(edgeLabels...).Statements[0])
	return g.lookup(ctx, req, lookupSpec{steps: steps, mode: lookupVertices, labels: edgeLabels, emitNull: emitNull})
}

func (g *FederatedGraph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	steps := moveSteps(gripql.NewQuery().In(edgeLabels...).Statements[0])
	return g.lookup(ctx, req, lookupSpec{steps: steps, mode: lookupVertices, labels: edgeLabels, fanout: true, emitNull: emitNull})
}

func (g *FederatedGraph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	steps := moveSteps(gripql.NewQuery().OutE(edgeLabels...).Statements[0])
	return g.lookup(ctx, req, lookupSpec{steps: steps, mode: lookupEdges, labels: edgeLabels, emitNull: emitNull})
}

func (g *FederatedGraph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	steps := moveSteps(gripql.NewQuery().InE(edgeLabels...).Statements[0])
	return g.lookup(ctx, req, lookupSpec{steps: steps, mode: lookupEdges, labels: edgeLabels, fanout: true, emitNull: emitNull})
}
