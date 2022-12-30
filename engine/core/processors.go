package core

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/copy"
	"github.com/influxdata/tdigest"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// LookupVerts starts query by looking on vertices
type LookupVerts struct {
	db       gdbi.GraphInterface
	ids      []string
	loadData bool
}

// Process LookupVerts
func (l *LookupVerts) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if len(l.ids) == 0 {
				for v := range l.db.GetVertexList(ctx, l.loadData) {
					out <- t.AddCurrent(&gdbi.DataElement{
						ID:     v.ID,
						Label:  v.Label,
						Data:   v.Data,
						Loaded: l.loadData,
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetVertex(i, l.loadData)
					if v != nil {
						out <- t.AddCurrent(&gdbi.DataElement{
							ID:     v.ID,
							Label:  v.Label,
							Data:   v.Data,
							Loaded: l.loadData,
						})
					}
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupVertsIndex look up vertices by indexed based feature
type LookupVertsIndex struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process LookupVertsIndex
func (l *LookupVertsIndex) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			for _, label := range l.labels {
				for id := range l.db.VertexLabelScan(ctx, label) {
					queryChan <- gdbi.ElementLookup{
						ID:  id,
						Ref: t,
					}
				}
			}
		}
	}()

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:     v.Vertex.ID,
				Label:  v.Vertex.Label,
				Data:   v.Vertex.Data,
				Loaded: v.Vertex.Loaded,
			})
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupEdges starts query by looking up edges
type LookupEdges struct {
	db       gdbi.GraphInterface
	ids      []string
	loadData bool
}

// Process runs LookupEdges
func (l *LookupEdges) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if len(l.ids) == 0 {
				for v := range l.db.GetEdgeList(ctx, l.loadData) {
					out <- t.AddCurrent(&gdbi.DataElement{
						ID:     v.ID,
						Label:  v.Label,
						From:   v.From,
						To:     v.To,
						Data:   v.Data,
						Loaded: v.Loaded,
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetEdge(i, l.loadData)
					if v != nil {
						out <- t.AddCurrent(&gdbi.DataElement{
							ID:     v.ID,
							Label:  v.Label,
							From:   v.From,
							To:     v.To,
							Data:   v.Data,
							Loaded: v.Loaded,
						})
					}
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupVertexAdjOut finds out vertex
type LookupVertexAdjOut struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process runs out vertex
func (l *LookupVertexAdjOut) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			if t.IsSignal() {
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
		for ov := range l.db.GetOutChannel(ctx, queryChan, l.loadData, l.labels) {
			if ov.IsSignal() {
				out <- ov.Ref
			} else {
				i := ov.Ref
				out <- i.AddCurrent(&gdbi.DataElement{
					ID:     ov.Vertex.ID,
					Label:  ov.Vertex.Label,
					Data:   ov.Vertex.Data,
					Loaded: ov.Vertex.Loaded,
				})
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
					ID:  t.GetCurrent().To,
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
				out <- i.AddCurrent(&gdbi.DataElement{
					ID:     v.Vertex.ID,
					Label:  v.Vertex.Label,
					Data:   v.Vertex.Data,
					Loaded: v.Vertex.Loaded,
				})
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
		for v := range l.db.GetInChannel(ctx, queryChan, l.loadData, l.labels) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(&gdbi.DataElement{
					ID:     v.Vertex.ID,
					Label:  v.Vertex.Label,
					Data:   v.Vertex.Data,
					Loaded: v.Vertex.Loaded,
				})
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
					ID:  t.GetCurrent().From,
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
				out <- i.AddCurrent(&gdbi.DataElement{
					ID:     v.Vertex.ID,
					Label:  v.Vertex.Label,
					Data:   v.Vertex.Data,
					Loaded: v.Vertex.Loaded,
				})
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
		for v := range l.db.GetInEdgeChannel(ctx, queryChan, l.loadData, l.labels) {
			i := v.Ref
			if i.IsSignal() {
				out <- i
			} else {
				out <- i.AddCurrent(&gdbi.DataElement{
					ID:     v.Edge.ID,
					To:     v.Edge.To,
					From:   v.Edge.From,
					Label:  v.Edge.Label,
					Data:   v.Edge.Data,
					Loaded: v.Edge.Loaded,
				})
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
		for v := range l.db.GetOutEdgeChannel(ctx, queryChan, l.loadData, l.labels) {
			i := v.Ref
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:     v.Edge.ID,
				To:     v.Edge.To,
				From:   v.Edge.From,
				Label:  v.Edge.Label,
				Data:   v.Edge.Data,
				Loaded: v.Edge.Loaded,
			})
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Fields selects fields from current element
type Fields struct {
	keys []string
}

// Process runs Values step
func (f *Fields) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			o := jsonpath.SelectTravelerFields(t, f.keys...)
			out <- o
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Render takes current state and renders into requested structure
type Render struct {
	Template interface{}
}

// Process runs the render processor
func (r *Render) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := jsonpath.RenderTraveler(t, r.Template)
			out <- &gdbi.BaseTraveler{Render: v}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Path tells system to return path data
type Path struct {
	Template interface{} //this isn't really used yet.
}

// Process runs the render processor
func (r *Path) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			out <- t
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Unwind takes an array field and replicates the message for every element in the array
type Unwind struct {
	Field string
}

// Process runs the render processor
func (r *Unwind) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := jsonpath.TravelerPathLookup(t, r.Field)
			if a, ok := v.([]interface{}); ok {
				cur := t.GetCurrent()
				if len(a) > 0 {
					for _, i := range a {
						o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: copy.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
						n := t.AddCurrent(&o)
						jsonpath.TravelerSetValue(n, r.Field, i)
						out <- n
					}
				} else {
					o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: copy.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
					n := t.AddCurrent(&o)
					jsonpath.TravelerSetValue(n, r.Field, nil)
					out <- n
				}
			} else {
				cur := t.GetCurrent()
				o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: copy.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
				n := t.AddCurrent(&o)
				jsonpath.TravelerSetValue(n, r.Field, nil)
				out <- n
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Has filters based on data
type Has struct {
	stmt *gripql.HasExpression
}

// Process runs Has
func (w *Has) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if logic.MatchesHasExpression(t, w.stmt) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasLabel filters elements based on their label.
type HasLabel struct {
	labels []string
}

// Process runs Count
func (h *HasLabel) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	labels := dedupStringSlice(h.labels)
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if contains(labels, t.GetCurrent().Label) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasKey filters elements based on whether it has one or more properties.
type HasKey struct {
	keys []string
}

// Process runs Count
func (h *HasKey) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		keys := dedupStringSlice(h.keys)
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			found := true
			for _, key := range keys {
				if !jsonpath.TravelerPathExists(t, key) {
					found = false
				}
			}
			if found {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasID filters elements based on their id.
type HasID struct {
	ids []string
}

// Process runs Count
func (h *HasID) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		ids := dedupStringSlice(h.ids)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if contains(ids, t.GetCurrentID()) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Count incoming elements
type Count struct{}

// Process runs Count
func (c *Count) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			i++
		}
		out <- &gdbi.BaseTraveler{Count: i}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Limit limits incoming values to count
type Limit struct {
	count uint32
}

// Process runs limit
func (l *Limit) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i < l.count {
				out <- t
			} else if i == l.count {
				cancel()
			}
			i++
		}
	}()
	return newCtx
}

////////////////////////////////////////////////////////////////////////////////

// Skip limits incoming values to count
type Skip struct {
	count uint32
}

// Process runs offset
func (o *Skip) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i >= o.count {
				out <- t
			}
			i++
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Range limits the number of travelers returned.
// When the low-end of the range is not met, objects are continued to be iterated.
// When within the low (inclusive) and high (exclusive) range, traversers are emitted.
// When above the high range, the traversal breaks out of iteration. Finally, the use of -1 on the high range will emit remaining traversers after the low range begins.
type Range struct {
	start int32
	stop  int32
}

// Process runs range
func (r *Range) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		var i int32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i >= r.start && (i < r.stop || r.stop == -1) {
				out <- t
			} else if i == r.stop {
				cancel()
			}
			i++
		}
	}()
	return newCtx
}

////////////////////////////////////////////////////////////////////////////////

// Distinct only returns unique objects as defined by the set of select features
type Distinct struct {
	vals []string
}

// Process runs distinct
func (g *Distinct) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		kv := man.GetTempKV()
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			s := make([][]byte, len(g.vals))
			found := true
			for i, v := range g.vals {
				if jsonpath.TravelerPathExists(t, v) {
					s[i] = []byte(fmt.Sprintf("%#v", jsonpath.TravelerPathLookup(t, v)))
				} else {
					found = false
				}
			}
			k := bytes.Join(s, []byte{0x00})
			if found && len(k) > 0 {
				if !kv.HasKey(k) {
					kv.Set(k, []byte{0x01})
					out <- t
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Marker marks the current element
type Marker struct {
	mark string
}

// Process runs Marker
func (m *Marker) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			out <- t.AddMark(m.mark, t.GetCurrent())
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Selector selects marks to return
type Selector struct {
	marks []string
}

// Process runs Selector
func (s *Selector) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			res := map[string]*gdbi.DataElement{}
			for _, mark := range s.marks {
				val := t.GetMark(mark)
				if val == nil {
					val = &gdbi.DataElement{}
				}
				res[mark] = val
			}
			out <- &gdbi.BaseTraveler{Selections: res}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

type ValueSet struct {
	key   string
	value interface{}
}

func (s *ValueSet) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			jsonpath.TravelerSetValue(t, s.key, s.value)
			out <- t
		}
	}()
	return ctx
}

type ValueIncrement struct {
	key   string
	value int32
}

func (s *ValueIncrement) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := jsonpath.TravelerPathLookup(t, s.key)
			i := cast.ToInt(v) + int(s.value)
			o := t.Copy()
			jsonpath.TravelerSetValue(o, s.key, i)
			out <- o
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// MarkSelect moves to selected mark
type MarkSelect struct {
	mark string
}

// Process runs Selector
func (s *MarkSelect) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			m := t.GetMark(s.mark)
			out <- t.AddCurrent(m)
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

////////////////////////////////////////////////////////////////////////////////

type aggregate struct {
	aggregations []*gripql.Aggregate
}

func (agg *aggregate) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	aChans := make(map[string](chan gdbi.Traveler))
	g, ctx := errgroup.WithContext(ctx)

	// # of travelers to buffer for agg
	bufferSize := 1000
	for _, a := range agg.aggregations {
		aChans[a.Name] = make(chan gdbi.Traveler, bufferSize)
	}

	g.Go(func() error {
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			for _, a := range agg.aggregations {
				aChans[a.Name] <- t
			}
		}
		for _, a := range agg.aggregations {
			if aChans[a.Name] != nil {
				close(aChans[a.Name])
				//aChans[a.Name] = nil
			}
		}
		return nil
	})

	for _, a := range agg.aggregations {
		a := a
		switch a.Aggregation.(type) {
		case *gripql.Aggregate_Term:
			g.Go(func() error {
				// max # of terms to collect before failing
				// since the term can be a string this still isn't particularly safe
				// the terms could be arbitrarily large strings and storing this many could eat up
				// lots of memory.
				maxTerms := 100000

				tagg := a.GetTerm()
				size := tagg.Size

				// Collect error to return. Because we are reading a channel, it must be fully emptied
				// If we return error before fully emptying channel, upstream processes will lock
				var outErr error
				fieldTermCounts := map[interface{}]int{}
				for t := range aChans[a.Name] {
					if len(fieldTermCounts) > maxTerms {
						outErr = fmt.Errorf("term aggreagtion: collected more unique terms (%v) than allowed (%v)", len(fieldTermCounts), maxTerms)
					} else {
						val := jsonpath.TravelerPathLookup(t, tagg.Field)
						if val != nil {
							k := reflect.TypeOf(val).Kind()
							if k != reflect.Array && k != reflect.Slice && k != reflect.Map {
								fieldTermCounts[val]++

							}
						}
					}
				}

				count := 0
				for term, tcount := range fieldTermCounts {
					if size <= 0 || count < int(size) {
						//sTerm, _ := structpb.NewValue(term)
						//fmt.Printf("Term: %s %s %d\n", a.Name, sTerm, tcount)
						out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
					}
				}
				return outErr
			})

		case *gripql.Aggregate_Histogram:

			g.Go(func() error {
				// max # of values to collect before failing
				maxValues := 10000000

				hagg := a.GetHistogram()
				i := float64(hagg.Interval)

				c := 0
				fieldValues := []float64{}

				// Collect error to return. Because we are reading a channel, it must be fully emptied
				// If we return error before fully emptying channel, upstream processes will lock
				var outErr error
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, hagg.Field)
					if val != nil {
						fval, err := cast.ToFloat64E(val)
						if err != nil {
							outErr = fmt.Errorf("histogram aggregation: can't convert %v to float64", val)
						}
						fieldValues = append(fieldValues, fval)
						if c > maxValues {
							outErr = fmt.Errorf("histogram aggreagtion: collected more values (%v) than allowed (%v)", c, maxValues)
						}
						c++
					}
				}
				sort.Float64s(fieldValues)
				min := fieldValues[0]
				max := fieldValues[len(fieldValues)-1]

				for bucket := math.Floor(min/i) * i; bucket <= max; bucket += i {
					var count float64
					for _, v := range fieldValues {
						if v >= bucket && v < (bucket+i) {
							count++
						}
					}
					//sBucket, _ := structpb.NewValue(bucket)
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: bucket, Value: float64(count)}}
				}
				return outErr
			})

		case *gripql.Aggregate_Percentile:

			g.Go(func() error {
				pagg := a.GetPercentile()
				percents := pagg.Percents

				var outErr error
				td := tdigest.New()
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, pagg.Field)
					fval, err := cast.ToFloat64E(val)
					if err != nil {
						outErr = fmt.Errorf("percentile aggregation: can't convert %v to float64", val)
					}
					td.Add(fval, 1)
				}

				for _, p := range percents {
					q := td.Quantile(p / 100)
					//sp, _ := structpb.NewValue(p)
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: p, Value: q}}
				}

				return outErr
			})

		case *gripql.Aggregate_Field:
			g.Go(func() error {
				fa := a.GetField()
				fieldCounts := map[interface{}]int{}
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, fa.Field)
					if m, ok := val.(map[string]interface{}); ok {
						for k := range m {
							fieldCounts[k]++
						}
					}
				}
				for term, tcount := range fieldCounts {
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
				}
				return nil
			})

		case *gripql.Aggregate_Type:
			g.Go(func() error {
				fa := a.GetType()
				fieldTypes := map[string]int{}
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, fa.Field)
					tname := gripql.GetFieldType(val)
					fieldTypes[tname]++
				}
				for term, tcount := range fieldTypes {
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
				}
				return nil
			})

		case *gripql.Aggregate_Count:
			g.Go(func() error {
				count := 0
				for range aChans[a.Name] {
					count++
				}
				out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: "count", Value: float64(count)}}
				return nil
			})

		default:
			log.Errorf("Error: unknown aggregation type: %T", a.Aggregation)
			continue
		}
	}

	go func() {
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("one or more aggregation failed")
		}
		close(out)
	}()

	return ctx
}
