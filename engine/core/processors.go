package core

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().ID,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range l.db.GetOutChannel(ctx, queryChan, l.loadData, l.labels) {
			i := ov.Ref
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:     ov.Vertex.ID,
				Label:  ov.Vertex.Label,
				Data:   ov.Vertex.Data,
				Loaded: ov.Vertex.Loaded,
			})
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().To,
				Ref: i,
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().ID,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetInChannel(ctx, queryChan, l.loadData, l.labels) {
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().From,
				Ref: i,
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().ID,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for v := range l.db.GetInEdgeChannel(ctx, queryChan, l.loadData, l.labels) {
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
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().ID,
				Ref: i,
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
			v := jsonpath.RenderTraveler(t, r.Template)
			out <- &gdbi.Traveler{Render: v}
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
			v := jsonpath.TravelerPathLookup(t, r.Field)
			if a, ok := v.([]interface{}); ok {
				cur := t.GetCurrent()
				if len(a) > 0 {
					for _, i := range a {
						o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: util.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
						n := t.AddCurrent(&o)
						jsonpath.TravelerSetValue(n, r.Field, i)
						out <- n
					}
				} else {
					o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: util.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
					n := t.AddCurrent(&o)
					jsonpath.TravelerSetValue(n, r.Field, nil)
					out <- n
				}
			} else {
				cur := t.GetCurrent()
				o := gdbi.DataElement{ID: cur.ID, Label: cur.Label, From: cur.From, To: cur.To, Data: util.DeepCopy(cur.Data).(map[string]interface{}), Loaded: true}
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

func matchesCondition(trav *gdbi.Traveler, cond *gripql.HasCondition) bool {
	var val interface{}
	var condVal interface{}
	val = jsonpath.TravelerPathLookup(trav, cond.Key)
	condVal = cond.Value.AsInterface()

	switch cond.Condition {
	case gripql.Condition_EQ:
		return reflect.DeepEqual(val, condVal)

	case gripql.Condition_NEQ:
		return !reflect.DeepEqual(val, condVal)

	case gripql.Condition_GT:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN > condN

	case gripql.Condition_GTE:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN >= condN

	case gripql.Condition_LT:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN < condN

	case gripql.Condition_LTE:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN <= condN

	case gripql.Condition_INSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Errorf("Error: could not cast INSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Errorf("Error: expected slice of length 2 not %v for INSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Errorf("Error: could not cast lower INSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Errorf("Error: could not cast upper INSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Errorf("Error: could not cast INSIDE value: %v", err)
			return false
		}
		return valF > lower && valF < upper

	case gripql.Condition_OUTSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Errorf("Error: could not cast OUTSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Errorf("Error: expected slice of length 2 not %v for OUTSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Errorf("Error: could not cast lower OUTSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Errorf("Error: could not cast upper OUTSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Errorf("Error: could not cast OUTSIDE value: %v", err)
			return false
		}
		return valF < lower || valF > upper

	case gripql.Condition_BETWEEN:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Errorf("Error: could not cast BETWEEN condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Errorf("Error: expected slice of length 2 not %v for BETWEEN condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Errorf("Error: could not cast lower BETWEEN condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Errorf("Error: could not cast upper BETWEEN condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Errorf("Error: could not cast BETWEEN value: %v", err)
			return false
		}
		return valF >= lower && valF < upper

	case gripql.Condition_WITHIN:
		found := false
		switch condVal := condVal.(type) {
		case []interface{}:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Errorf("Error: expected slice not %T for WITHIN condition value", condVal)
		}

		return found

	case gripql.Condition_WITHOUT:
		found := false
		switch condVal := condVal.(type) {
		case []interface{}:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Errorf("Error: expected slice not %T for WITHOUT condition value", condVal)

		}

		return !found

	case gripql.Condition_CONTAINS:
		found := false
		switch val := val.(type) {
		case []interface{}:
			for _, v := range val {
				if reflect.DeepEqual(v, condVal) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Errorf("Error: unknown condition value type %T for CONTAINS condition", val)
		}

		return found

	default:
		return false
	}
}

func matchesHasExpression(trav *gdbi.Traveler, stmt *gripql.HasExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		return matchesCondition(trav, cond)

	case *gripql.HasExpression_And:
		and := stmt.GetAnd()
		andRes := []bool{}
		for _, e := range and.Expressions {
			andRes = append(andRes, matchesHasExpression(trav, e))
		}
		for _, r := range andRes {
			if !r {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		or := stmt.GetOr()
		orRes := []bool{}
		for _, e := range or.Expressions {
			orRes = append(orRes, matchesHasExpression(trav, e))
		}
		for _, r := range orRes {
			if r {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		e := stmt.GetNot()
		return !matchesHasExpression(trav, e)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}

// Process runs Has
func (w *Has) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if matchesHasExpression(t, w.stmt) {
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
			if contains(ids, t.GetCurrent().ID) {
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
		for range in {
			i++
		}
		out <- &gdbi.Traveler{Count: i}
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
			res := map[string]*gdbi.DataElement{}
			for _, mark := range s.marks {
				val := t.GetMark(mark)
				if val == nil {
					val = &gdbi.DataElement{}
				}
				res[mark] = val
			}
			out <- &gdbi.Traveler{Selections: res}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Jump moves to selected mark
type Jump struct {
	mark string
}

// Process runs Selector
func (s *Jump) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
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
		chanIn := make([]chan *gdbi.Traveler, len(procs))
		chanOut := make([]chan *gdbi.Traveler, len(procs))
		for i := range procs {
			chanIn[i] = make(chan *gdbi.Traveler, 1000)
			chanOut[i] = make(chan *gdbi.Traveler, 1000)
		}
		for i, p := range procs {
			p.Process(ctx, man, chanIn[i], chanOut[i])
		}
		for t := range in {
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
	aChans := make(map[string](chan *gdbi.Traveler))
	g, ctx := errgroup.WithContext(ctx)

	// # of travelers to buffer for agg
	bufferSize := 1000
	for _, a := range agg.aggregations {
		aChans[a.Name] = make(chan *gdbi.Traveler, bufferSize)
	}

	g.Go(func() error {
		for t := range in {
			for _, a := range agg.aggregations {
				aChans[a.Name] <- t
			}
		}
		for _, a := range agg.aggregations {
			if aChans[a.Name] != nil {
				close(aChans[a.Name])
				aChans[a.Name] = nil
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

				fieldTermCounts := map[interface{}]int{}
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, tagg.Field)
					if val != nil {
						k := reflect.TypeOf(val).Kind()
						if k != reflect.Array && k != reflect.Slice && k != reflect.Map {
							fieldTermCounts[val]++
							if len(fieldTermCounts) > maxTerms {
								return fmt.Errorf("term aggreagtion: collected more unique terms (%v) than allowed (%v)", len(fieldTermCounts), maxTerms)
							}
						}
					}
				}

				count := 0
				for term, tcount := range fieldTermCounts {
					if size <= 0 || count < int(size) {
						//sTerm, _ := structpb.NewValue(term)
						//fmt.Printf("Term: %s %s %d\n", a.Name, sTerm, tcount)
						out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
					}
				}
				return nil
			})

		case *gripql.Aggregate_Histogram:

			g.Go(func() error {
				// max # of values to collect before failing
				maxValues := 10000000

				hagg := a.GetHistogram()
				i := float64(hagg.Interval)

				c := 0
				fieldValues := []float64{}
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, hagg.Field)
					if val != nil {
						fval, err := cast.ToFloat64E(val)
						if err != nil {
							return fmt.Errorf("histogram aggregation: can't convert %v to float64", val)
						}
						fieldValues = append(fieldValues, fval)
						if c > maxValues {
							return fmt.Errorf("histogram aggreagtion: collected more values (%v) than allowed (%v)", c, maxValues)
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
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: bucket, Value: float64(count)}}
				}
				return nil
			})

		case *gripql.Aggregate_Percentile:

			g.Go(func() error {
				pagg := a.GetPercentile()
				percents := pagg.Percents

				td := tdigest.New()
				for t := range aChans[a.Name] {
					val := jsonpath.TravelerPathLookup(t, pagg.Field)
					fval, err := cast.ToFloat64E(val)
					if err != nil {
						return fmt.Errorf("percentile aggregation: can't convert %v to float64", val)
					}
					td.Add(fval, 1)
				}

				for _, p := range percents {
					q := td.Quantile(p / 100)
					//sp, _ := structpb.NewValue(p)
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: p, Value: q}}
				}

				return nil
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
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
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
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
				}
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
