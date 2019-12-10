package core

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/spenczar/tdigest"
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
						ID:    v.Gid,
						Label: v.Label,
						Data:  protoutil.AsMap(v.Data),
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetVertex(i, l.loadData)
					if v != nil {
						out <- t.AddCurrent(&gdbi.DataElement{
							ID:    v.Gid,
							Label: v.Label,
							Data:  protoutil.AsMap(v.Data),
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
		for v := range l.db.GetVertexChannel(queryChan, l.loadData) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
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
						ID:    v.Gid,
						Label: v.Label,
						From:  v.From,
						To:    v.To,
						Data:  protoutil.AsMap(v.Data),
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetEdge(i, l.loadData)
					if v != nil {
						out <- t.AddCurrent(&gdbi.DataElement{
							ID:    v.Gid,
							Label: v.Label,
							From:  v.From,
							To:    v.To,
							Data:  protoutil.AsMap(v.Data),
						})
					}
				}
			}
		}
	}()
	return ctx
}


// LookupVertsIndex look up vertices by indexed based feature
type LookupIndex struct {
	db       gdbi.GraphInterface
	query    *gripql.IndexQuery
	loadData bool
}

// Process LookupVertsIndex
func (l *LookupIndex) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for range in {}

	}()

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(queryChan, l.loadData) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
			})
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
		for ov := range l.db.GetOutChannel(queryChan, l.loadData, l.labels) {
			i := ov.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    ov.Vertex.Gid,
				Label: ov.Vertex.Label,
				Data:  protoutil.AsMap(ov.Vertex.Data),
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
		for v := range l.db.GetVertexChannel(queryChan, l.loadData) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
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
		for v := range l.db.GetInChannel(queryChan, l.loadData, l.labels) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
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
		for v := range l.db.GetVertexChannel(queryChan, l.loadData) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
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
		for v := range l.db.GetInEdgeChannel(queryChan, l.loadData, l.labels) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Edge.Gid,
				To:    v.Edge.To,
				From:  v.Edge.From,
				Label: v.Edge.Label,
				Data:  protoutil.AsMap(v.Edge.Data),
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
		for v := range l.db.GetOutEdgeChannel(queryChan, l.loadData, l.labels) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Edge.Gid,
				To:    v.Edge.To,
				From:  v.Edge.From,
				Label: v.Edge.Label,
				Data:  protoutil.AsMap(v.Edge.Data),
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

// Has filters based on data
type Has struct {
	stmt *gripql.HasExpression
}

func matchesCondition(trav *gdbi.Traveler, cond *gripql.HasCondition) bool {
	var val interface{}
	var condVal interface{}
	val = jsonpath.TravelerPathLookup(trav, cond.Key)
	condVal = protoutil.UnWrapValue(cond.Value)

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
		switch condVal.(type) {
		case []interface{}:
			condL, ok := condVal.([]interface{})
			if !ok {
				return false
			}
			for _, v := range condL {
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
		switch condVal.(type) {
		case []interface{}:
			condL, ok := condVal.([]interface{})
			if !ok {
				return false
			}
			for _, v := range condL {
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
		switch val.(type) {
		case []interface{}:
			valL, ok := val.([]interface{})
			if !ok {
				return false
			}
			for _, v := range valL {
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
			for i, v := range g.vals {
				if jsonpath.TravelerPathExists(t, v) {
					s[i] = []byte(fmt.Sprintf("%#v", jsonpath.TravelerPathLookup(t, v)))
				}
			}
			k := bytes.Join(s, []byte{0x00})
			if len(k) > 0 {
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
			out <- t.AddCurrent(t.GetMark(s.mark))
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
	aChans := make(map[string](chan []*gdbi.Traveler))
	g, ctx := errgroup.WithContext(ctx)

	go func() {
		for _, a := range agg.aggregations {
			aChans[a.Name] = make(chan []*gdbi.Traveler, 100)
			defer close(aChans[a.Name])
		}

		batchSize := 100
		i := 0
		batch := []*gdbi.Traveler{}
		for t := range in {
			if i == batchSize {
				for _, a := range agg.aggregations {
					aChans[a.Name] <- batch
				}
				i = 0
				batch = []*gdbi.Traveler{}
			}
			batch = append(batch, t)
			i++
		}
		for _, a := range agg.aggregations {
			aChans[a.Name] <- batch
		}
	}()

	aggChan := make(chan map[string]*gripql.AggregationResult, len(agg.aggregations))
	for _, a := range agg.aggregations {
		a := a
		switch a.Aggregation.(type) {
		case *gripql.Aggregate_Term:
			g.Go(func() error {
				tagg := a.GetTerm()
				size := tagg.Size
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(tagg.Field)
				field := jsonpath.GetJSONPath(tagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						return err
					}
				}

				aggOut := &gripql.AggregationResult{
					Buckets: []*gripql.AggregationResultBucket{},
				}

				for tcount := range idx.FieldTermCounts(field) {
					var t *structpb.Value
					if tcount.String != "" {
						t = protoutil.WrapValue(tcount.String)
					} else {
						t = protoutil.WrapValue(tcount.Number)
					}
					aggOut.SortedInsert(&gripql.AggregationResultBucket{Key: t, Value: float64(tcount.Count)})
					if size > 0 {
						if len(aggOut.Buckets) > int(size) {
							aggOut.Buckets = aggOut.Buckets[:size]
						}
					}
				}

				aggChan <- map[string]*gripql.AggregationResult{a.Name: aggOut}
				return nil
			})

		case *gripql.Aggregate_Histogram:
			g.Go(func() error {
				hagg := a.GetHistogram()
				interval := hagg.Interval
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(hagg.Field)
				field := jsonpath.GetJSONPath(hagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						return err
					}
				}

				aggOut := &gripql.AggregationResult{
					Buckets: []*gripql.AggregationResultBucket{},
				}

				min := idx.FieldTermNumberMin(field)
				max := idx.FieldTermNumberMax(field)

				i := float64(interval)
				for bucket := math.Floor(min/i) * i; bucket <= max; bucket += i {
					var count uint64
					for tcount := range idx.FieldTermNumberRange(field, bucket, bucket+i) {
						count += tcount.Count
					}
					aggOut.Buckets = append(aggOut.Buckets, &gripql.AggregationResultBucket{Key: protoutil.WrapValue(bucket), Value: float64(count)})
				}

				aggChan <- map[string]*gripql.AggregationResult{a.Name: aggOut}
				return nil
			})

		case *gripql.Aggregate_Percentile:

			g.Go(func() error {
				pagg := a.GetPercentile()
				percents := pagg.Percents
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(pagg.Field)
				field := jsonpath.GetJSONPath(pagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						return err
					}
				}

				aggOut := &gripql.AggregationResult{
					Buckets: []*gripql.AggregationResultBucket{},
				}

				td := tdigest.New()
				for val := range idx.FieldNumbers(field) {
					td.Add(val, 1)
				}

				for _, p := range percents {
					q := td.Quantile(p / 100)
					aggOut.Buckets = append(aggOut.Buckets, &gripql.AggregationResultBucket{Key: protoutil.WrapValue(p), Value: q})
				}

				aggChan <- map[string]*gripql.AggregationResult{a.Name: aggOut}
				return nil
			})

		default:
			log.Errorf("Error: unknown aggregation type: %T", a.Aggregation)
			continue
		}
	}

	// Check whether any goroutines failed.
	go func() {
		defer close(out)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("one or more aggregation failed")
		}
		close(aggChan)
		aggs := map[string]*gripql.AggregationResult{}
		for a := range aggChan {
			for k, v := range a {
				aggs[k] = v
			}
		}
		out <- &gdbi.Traveler{Aggregations: aggs}
	}()

	return ctx
}
