package psqlx

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type Graph struct {
	base gdbi.GraphInterface
	conf Config
	graph string
	exec  DelegatedExecutor
}

func (g *Graph) Compiler() gdbi.Compiler {
	return newDelegatedCompiler(g.base.Compiler(), g.conf, g.graph, g.exec)
}

func (g *Graph) Traversal(ctx context.Context, stmts []*gripql.GraphStatement) (<-chan *gripql.QueryResult, error) {
	if !g.conf.DelegateTraversal {
		return nil, fmt.Errorf("psqlx: delegated traversal is disabled")
	}
	if err := validateDelegatedStatementSubset(stmts); err != nil {
		return nil, err
	}
	if g.exec == nil {
		return nil, fmt.Errorf("psqlx: delegated traversal executor is unavailable")
	}
	req, err := buildDelegatedRequest(g.conf, stmts)
	if err != nil {
		return nil, err
	}
	rows, err := g.exec.Execute(ctx, g.graph, req)
	if err != nil {
		return nil, err
	}
	out := make(chan *gripql.QueryResult, len(rows))
	go func() {
		defer close(out)
		for _, r := range rows {
			out <- r
		}
	}()
	return out, nil
}

func (g *Graph) GetTimestamp() string {
	return g.base.GetTimestamp()
}

func (g *Graph) GetVertex(key string, load bool) *gdbi.Vertex {
	return g.base.GetVertex(key, load)
}

func (g *Graph) GetEdge(key string, load bool) *gdbi.Edge {
	return g.base.GetEdge(key, load)
}

func (g *Graph) AddVertex(vertex []*gdbi.Vertex) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: AddVertex is not supported in read-only mode")
	}
	return g.base.AddVertex(vertex)
}

func (g *Graph) AddEdge(edge []*gdbi.Edge) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: AddEdge is not supported in read-only mode")
	}
	return g.base.AddEdge(edge)
}

func (g *Graph) BulkAdd(in <-chan *gdbi.GraphElement) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: BulkAdd is not supported in read-only mode")
	}
	return g.base.BulkAdd(in)
}

func (g *Graph) BulkDel(data *gdbi.DeleteData) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: BulkDel is not supported in read-only mode")
	}
	return g.base.BulkDel(data)
}

func (g *Graph) DelVertex(key string) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: DelVertex is not supported in read-only mode")
	}
	return g.base.DelVertex(key)
}

func (g *Graph) DelEdge(key string) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: DelEdge is not supported in read-only mode")
	}
	return g.base.DelEdge(key)
}

func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	return g.base.VertexLabelScan(ctx, label)
}

func (g *Graph) ListVertexLabels() ([]string, error) {
	return g.base.ListVertexLabels()
}

func (g *Graph) ListEdgeLabels() ([]string, error) {
	return g.base.ListEdgeLabels()
}

func (g *Graph) AddVertexIndex(label string, field string) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: AddVertexIndex is not supported in read-only mode")
	}
	return g.base.AddVertexIndex(label, field)
}

func (g *Graph) DeleteVertexIndex(label string, field string) error {
	if g.conf.ReadOnly {
		return fmt.Errorf("psqlx: DeleteVertexIndex is not supported in read-only mode")
	}
	return g.base.DeleteVertexIndex(label, field)
}

func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	return g.base.GetVertexIndexList()
}

func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	return g.base.GetVertexList(ctx, load)
}

func (g *Graph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	return g.base.GetVertexChannel(ctx, req, load)
}

func (g *Graph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.base.GetOutChannel(ctx, req, load, emitNull, edgeLabels)
}

func (g *Graph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.base.GetInChannel(ctx, req, load, emitNull, edgeLabels)
}

func (g *Graph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.base.GetOutEdgeChannel(ctx, req, load, emitNull, edgeLabels)
}

func (g *Graph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.base.GetInEdgeChannel(ctx, req, load, emitNull, edgeLabels)
}
