package grids

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/util/setcmp"
)

// ElementLookup request to look up data
type RawElementLookup struct {
	ID      uint64
	Ref     *GRIDTraveler
	Element *GRIDDataElement
}

func (rel *RawElementLookup) IsSignal() bool {
	if rel.Ref != nil {
		return rel.Ref.IsSignal()
	}
	return false
}

type PathVProc struct {
	db  *Graph
	ids []string
}

func (r *PathVProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		for range in {
		}
		defer close(out)
		if len(r.ids) == 0 {
			for elem := range r.db.RawGetVertexList(ctx) {
				out <- &GRIDTraveler{Current: elem, Graph: r.db}
			}
		} else {
			for _, i := range r.ids {
				if key, ok := r.db.keyMap.GetVertexKey(i); ok {
					label := r.db.keyMap.GetVertexLabel(key)
					o := &GRIDTraveler{
						Current: &GRIDDataElement{Gid: key, Label: label, Data: map[string]interface{}{}, Loaded: false},
						Graph:   r.db,
					}
					out <- o
				}
			}
		}
	}()
	return ctx
}

type PathOutProc struct {
	db     *Graph
	labels []string
}

func (r *PathOutProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, true, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.Gid,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetOutChannel(queryChan, r.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

type PathInProc struct {
	db     *Graph
	labels []string
}

func (r *PathInProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, true, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.Gid,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetInChannel(queryChan, r.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

type PathOutEProc struct {
	db     *Graph
	labels []string
}

func (r *PathOutEProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, false, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.Gid,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetOutEdgeChannel(queryChan, r.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

// PathOutAdjEProc process edge to out
type PathOutEdgeAdjProc struct {
	db *Graph
}

func (r *PathOutEdgeAdjProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, true, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.To,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetVertexChannel(queryChan) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

type PathInEProc struct {
	db     *Graph
	labels []string
}

func (r *PathInEProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, true, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.Gid,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetInEdgeChannel(queryChan, r.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

type PathInEdgeAdjProc struct {
	db     *Graph
	labels []string
}

func (r *PathInEdgeAdjProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			gi := NewGRIDTraveler(i, true, r.db)
			if gi.IsSignal() {
				queryChan <- &RawElementLookup{
					Ref: gi,
				}
			} else {
				queryChan <- &RawElementLookup{
					ID:  gi.Current.From,
					Ref: gi,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetVertexChannel(queryChan) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddRawCurrent(ov.Element)
			}
		}
	}()
	return ctx
}

type PathLabelProc struct {
	db     *Graph
	labels []string
}

func (r *PathLabelProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	labels := []uint64{}
	for i := range r.labels {
		if j, ok := r.db.keyMap.GetLabelKey(r.labels[i]); ok {
			labels = append(labels, j)
		}
	}
	go func() {
		defer close(out)
		for i := range in {
			if i.IsSignal() {
				out <- i
			}
			gi := NewGRIDTraveler(i, true, r.db)
			if setcmp.ContainsUint(labels, gi.Current.Label) {
				out <- i
			}
		}
	}()
	return ctx
}
