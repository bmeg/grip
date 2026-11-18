package node

import (
	"context"
	"strings"
	"sync"

	"github.com/bmeg/grip/gdbi"
)

// BoundaryGraph wraps the graph served by a resource node. Moves that reach a
// vertex owned by another source return a bare reference (ID only, not loaded)
// instead of looking the vertex up, so the traversal can hand the traveler off.
type BoundaryGraph struct {
	gdbi.GraphInterface
	Prefix string
}

func NewBoundaryGraph(inner gdbi.GraphInterface, prefix string) *BoundaryGraph {
	return &BoundaryGraph{GraphInterface: inner, Prefix: prefix}
}

// Owns reports whether the ID belongs to this source.
func (b *BoundaryGraph) Owns(id string) bool {
	return strings.HasPrefix(id, b.Prefix)
}

// GetVertexChannel loads owned vertices from the wrapped graph and answers
// foreign IDs with an unloaded reference.
func (b *BoundaryGraph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	local := make(chan gdbi.ElementLookup, 100)
	inner := b.GraphInterface.GetVertexChannel(ctx, local, load)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := range inner {
			out <- r
		}
	}()
	go func() {
		defer wg.Done()
		defer close(local)
		for r := range req {
			if r.IsSignal() || r.ID == "" || b.Owns(r.ID) {
				local <- r
			} else {
				r.Vertex = &gdbi.DataElement{ID: r.ID}
				out <- r
			}
		}
	}()
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// GetOutChannel follows outgoing edges stored in the wrapped graph and returns
// the destination vertices, which may be foreign references.
func (b *BoundaryGraph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	edges := b.GraphInterface.GetOutEdgeChannel(ctx, req, false, emitNull, edgeLabels)
	return b.edgeTargets(ctx, edges, load, func(e *gdbi.DataElement) string { return e.To })
}

// GetInChannel follows incoming edges stored in the wrapped graph and returns
// their source vertices, which may be foreign references.
func (b *BoundaryGraph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	edges := b.GraphInterface.GetInEdgeChannel(ctx, req, false, emitNull, edgeLabels)
	return b.edgeTargets(ctx, edges, load, func(e *gdbi.DataElement) string { return e.From })
}

func (b *BoundaryGraph) edgeTargets(ctx context.Context, edges chan gdbi.ElementLookup, load bool, target func(*gdbi.DataElement) string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	vreq := make(chan gdbi.ElementLookup, 100)
	verts := b.GetVertexChannel(ctx, vreq, load)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for v := range verts {
			out <- v
		}
	}()
	go func() {
		defer wg.Done()
		defer close(vreq)
		for el := range edges {
			if el.IsSignal() {
				vreq <- el
				continue
			}
			if el.Edge == nil {
				// null move requested by the step: pass it on with no vertex
				out <- gdbi.ElementLookup{Ref: el.Ref}
				continue
			}
			vreq <- gdbi.ElementLookup{ID: target(el.Edge.Get()), Ref: el.Ref}
		}
	}()
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
