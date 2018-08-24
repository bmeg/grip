package core

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/spenczar/tdigest"
	"golang.org/x/sync/errgroup"
)

type propKey string

var propLoad propKey = "load"

func getPropLoad(ctx context.Context) bool {
	v := ctx.Value(propLoad)
	if v == nil {
		return true
	}
	return v.(bool)
}

////////////////////////////////////////////////////////////////////////////////

// LookupVerts starts query by looking on vertices
type LookupVerts struct {
	db  gdbi.GraphInterface
	ids []string
}

// Process LookupVerts
func (l *LookupVerts) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if len(l.ids) == 0 {
				for v := range l.db.GetVertexList(ctx, getPropLoad(ctx)) {
					out <- t.AddCurrent(&gdbi.DataElement{
						ID:    v.Gid,
						Label: v.Label,
						Data:  protoutil.AsMap(v.Data),
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetVertex(i, getPropLoad(ctx))
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
	db     gdbi.GraphInterface
	labels []string
}

// Process LookupVertsIndex
func (l *LookupVertsIndex) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			for _, label := range l.labels {
				for id := range l.db.VertexLabelScan(context.Background(), label) {
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
		for v := range l.db.GetVertexChannel(queryChan, getPropLoad(ctx)) {
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
	db  gdbi.GraphInterface
	ids []string
}

// Process runs LookupEdges
func (l *LookupEdges) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if len(l.ids) == 0 {
				for v := range l.db.GetEdgeList(context.Background(), getPropLoad(ctx)) {
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
					v := l.db.GetEdge(i, getPropLoad(ctx))
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

////////////////////////////////////////////////////////////////////////////////

// LookupVertexAdjOut finds out vertex
type LookupVertexAdjOut struct {
	db     gdbi.GraphInterface
	labels []string
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
		for ov := range l.db.GetOutChannel(queryChan, getPropLoad(ctx), l.labels) {
			i := ov.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    ov.Vertex.Gid,
				Label: ov.Vertex.Label,
				Data:  protoutil.AsMap(ov.Vertex.Data),
			})
		}
	}()
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// LookupEdgeAdjOut finds out edge
type LookupEdgeAdjOut struct {
	db     gdbi.GraphInterface
	labels []string
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
		for v := range l.db.GetVertexChannel(queryChan, getPropLoad(ctx)) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
			})
		}
	}()
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// LookupVertexAdjIn finds incoming vertex
type LookupVertexAdjIn struct {
	db     gdbi.GraphInterface
	labels []string
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
		for v := range l.db.GetInChannel(queryChan, getPropLoad(ctx), l.labels) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
			})
		}
	}()
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// LookupEdgeAdjIn finds incoming edge
type LookupEdgeAdjIn struct {
	db     gdbi.GraphInterface
	labels []string
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
		for v := range l.db.GetVertexChannel(queryChan, getPropLoad(ctx)) {
			i := v.Ref.(*gdbi.Traveler)
			out <- i.AddCurrent(&gdbi.DataElement{
				ID:    v.Vertex.Gid,
				Label: v.Vertex.Label,
				Data:  protoutil.AsMap(v.Vertex.Data),
			})
		}
	}()
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// InEdge finds the incoming edges
type InEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

// Process runs InEdge
func (l *InEdge) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetInEdgeChannel(queryChan, getPropLoad(ctx), l.labels) {
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
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// OutEdge finds the outgoing edges
type OutEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

// Process runs OutEdge
func (l *OutEdge) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetOutEdgeChannel(queryChan, getPropLoad(ctx), l.labels) {
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
	return context.WithValue(ctx, propLoad, false)
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
			if len(f.keys) == 0 {
				out <- t
			} else {
				o, err := jsonpath.SelectTravelerFields(t, f.keys...)
				if err != nil {
					log.Printf("error selecting fields: %v for traveler %+v", f.keys, t)
					continue
				}
				out <- o
			}
		}
	}()
	return context.WithValue(ctx, propLoad, true)
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
	return context.WithValue(ctx, propLoad, true)
}

////////////////////////////////////////////////////////////////////////////////

// Where filters based on data
type Where struct {
	stmt *gripql.WhereExpression
}

func matchesCondition(trav *gdbi.Traveler, cond *gripql.WhereCondition) bool {
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

	case gripql.Condition_IN:
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
			log.Printf("Error: expected slice not %T for IN condition value", condVal)
		}

		return found

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
			log.Printf("Error: unknown condition value type %T for CONTAINS condition", val)
		}

		return found

	default:
		return false
	}
}

func matchesWhereExpression(trav *gdbi.Traveler, stmt *gripql.WhereExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.WhereExpression_Condition:
		cond := stmt.GetCondition()
		return matchesCondition(trav, cond)

	case *gripql.WhereExpression_And:
		and := stmt.GetAnd()
		andRes := []bool{}
		for _, e := range and.Expressions {
			andRes = append(andRes, matchesWhereExpression(trav, e))
		}
		for _, r := range andRes {
			if !r {
				return false
			}
		}
		return true

	case *gripql.WhereExpression_Or:
		or := stmt.GetOr()
		orRes := []bool{}
		for _, e := range or.Expressions {
			orRes = append(orRes, matchesWhereExpression(trav, e))
		}
		for _, r := range orRes {
			if r {
				return true
			}
		}
		return false

	case *gripql.WhereExpression_Not:
		e := stmt.GetNot()
		return !matchesWhereExpression(trav, e)

	default:
		log.Printf("unknown where expression type")
		return false
	}
}

// Process runs Where
func (w *Where) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if matchesWhereExpression(t, w.stmt) {
				out <- t
			}
		}
	}()
	return context.WithValue(ctx, propLoad, true)
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
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if i == l.count {
				return
			}
			out <- t
			i++
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Offset limits incoming values to count
type Offset struct {
	count uint32
}

// Process runs offset
func (o *Offset) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
	return context.WithValue(ctx, propLoad, true)
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
	return context.WithValue(ctx, propLoad, true)
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

type both struct {
	db       gdbi.GraphInterface
	labels   []string
	lastType gdbi.DataType
	toType   gdbi.DataType
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
					&InEdge{b.db, b.labels},
					&OutEdge{b.db, b.labels},
				}
			default:
				procs = []gdbi.Processor{
					&LookupVertexAdjIn{b.db, b.labels},
					&LookupVertexAdjOut{b.db, b.labels},
				}
			}
		case gdbi.EdgeData:
			procs = []gdbi.Processor{
				&LookupEdgeAdjIn{b.db, b.labels},
				&LookupEdgeAdjOut{b.db, b.labels},
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
	return context.WithValue(ctx, propLoad, false)
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
		return
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

				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							if doc["label"] == tagg.Label {
								err := idx.AddDocTx(tx, doc["gid"].(string), doc)
								if err != nil {
									return err
								}
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

				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							if doc["label"] == hagg.Label {
								err := idx.AddDocTx(tx, doc["gid"].(string), doc)
								if err != nil {
									return err
								}
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

				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							if doc["label"] == pagg.Label {
								err := idx.AddDocTx(tx, doc["gid"].(string), doc)
								if err != nil {
									return err
								}
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
			log.Println("Error: unknown aggregation type")
			continue
		}
	}

	// Check whether any goroutines failed.
	go func() {
		defer close(out)
		if err := g.Wait(); err != nil {
			log.Printf("Error: one or more aggregation failed: %v", err)
		}
		close(aggChan)
		aggs := map[string]*gripql.AggregationResult{}
		for a := range aggChan {
			for k, v := range a {
				aggs[k] = v
			}
		}
		out <- &gdbi.Traveler{Aggregations: aggs}
		return
	}()

	return context.WithValue(ctx, propLoad, true)
}
