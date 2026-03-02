package core

import (
	"context"

	"github.com/bmeg/grip/gdbi"
)

////////////////////////////////////////////////////////////////////////////////

// LookupVertexAdjOut finds out vertex
type LookupVertexAdjOut struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
	emitNull bool
}

// Process runs out vertex
func (l *LookupVertexAdjOut) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() || t.IsNull() {
				queryChan <- gdbi.ElementLookup{
					Ref: t,
				}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrentID(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range l.db.GetOutChannel(ctx, queryChan, l.loadData, l.emitNull, l.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddCurrent(ov.Vertex)
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupEdgeAdjOut finds out edge
type LookupEdgeAdjOut struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process runs LookupEdgeAdjOut
func (l *LookupEdgeAdjOut) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
				queryChan <- gdbi.ElementLookup{Ref: t}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrent().GetTo(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(v.Vertex)
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupVertexAdjIn finds incoming vertex
type LookupVertexAdjIn struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
	emitNull bool
}

// Process runs LookupVertexAdjIn
func (l *LookupVertexAdjIn) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
				queryChan <- gdbi.ElementLookup{Ref: t}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrentID(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetInChannel(ctx, queryChan, l.loadData, l.emitNull, l.labels) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(v.Vertex)
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupEdgeAdjIn finds incoming edge
type LookupEdgeAdjIn struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process runs LookupEdgeAdjIn
func (l *LookupEdgeAdjIn) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
				queryChan <- gdbi.ElementLookup{Ref: t}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrent().GetFrom(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(v.Vertex)
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// InE finds the incoming edges
type InE struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
	emitNull bool
}

// Process runs InE
func (l *InE) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
				queryChan <- gdbi.ElementLookup{Ref: t}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrentID(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetInEdgeChannel(ctx, queryChan, l.loadData, l.emitNull, l.labels) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(v.Edge)
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// OutE finds the outgoing edges
type OutE struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
	emitNull bool
}

// Process runs OutE
func (l *OutE) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
				queryChan <- gdbi.ElementLookup{Ref: t}
			} else {
				queryChan <- gdbi.ElementLookup{
					ID:  t.GetCurrentID(),
					Ref: t,
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetOutEdgeChannel(ctx, queryChan, l.loadData, l.emitNull, l.labels) {
			i := v.Ref
			out <- i.AddCurrent(v.Edge)
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

type both struct {
	db       gdbi.GraphInterface
	labels   []string
	lastType gdbi.DataType
	toType   gdbi.DataType
	loadData bool
}

func (b both) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var procs []gdbi.Processor
		switch b.lastType {
		case gdbi.VertexData:
			switch b.toType {
			case gdbi.EdgeData:
				procs = []gdbi.Processor{
					&InE{db: b.db, loadData: b.loadData, labels: b.labels},
					&OutE{db: b.db, loadData: b.loadData, labels: b.labels},
				}
			default:
				procs = []gdbi.Processor{
					&LookupVertexAdjIn{db: b.db, labels: b.labels, loadData: b.loadData},
					&LookupVertexAdjOut{db: b.db, labels: b.labels, loadData: b.loadData},
				}
			}
		case gdbi.EdgeData:
			procs = []gdbi.Processor{
				&LookupEdgeAdjIn{db: b.db, labels: b.labels, loadData: b.loadData},
				&LookupEdgeAdjOut{db: b.db, labels: b.labels, loadData: b.loadData},
			}
		}
		chanIn := make([]chan gdbi.Traveler, len(procs))
		chanOut := make([]chan gdbi.Traveler, len(procs))
		for i := range procs {
			chanIn[i] = make(chan gdbi.Traveler, 1000)
			chanOut[i] = make(chan gdbi.Traveler, 1000)
		}
		for i, p := range procs {
			p.Process(ctx, man, chanIn[i], chanOut[i])
		}
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			for _, ch := range chanIn {
				ch <- t
			}
		}
		for _, ch := range chanIn {
			close(ch)
		}
		for i := range procs {
			for c := range chanOut[i] {
				out <- c
			}
		}
	}()
	return ctx
}
