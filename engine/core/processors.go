package core

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/jsengine"
	_ "github.com/bmeg/arachne/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/otto" // import otto so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/v8"   // import v8 so it registers with the driver map
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
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
				o, err := t.SelectFields(f.keys...)
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
	template interface{}
}

// Process runs the render processor
func (r *Render) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			v := jsonpath.Render(r.template, t)
			o := t.AddCurrent(&gdbi.DataElement{
				Value: v,
			})
			out <- o
		}
	}()
	return context.WithValue(ctx, propLoad, true)
}

////////////////////////////////////////////////////////////////////////////////

// Where filters based on data
type Where struct {
	stmt *aql.WhereExpression
}

func matchesCondition(trav *gdbi.Traveler, cond *aql.WhereCondition) bool {
	var val interface{}
	var condVal interface{}
	val = jsonpath.Render(cond.Key, trav)
	condVal = protoutil.UnWrapValue(cond.Value)

	switch cond.Condition {
	case aql.Condition_EQ:
		return reflect.DeepEqual(val, condVal)

	case aql.Condition_NEQ:
		return !reflect.DeepEqual(val, condVal)

	case aql.Condition_GT:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN > condN

	case aql.Condition_GTE:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN >= condN

	case aql.Condition_LT:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN < condN

	case aql.Condition_LTE:
		valN, ok := val.(float64)
		if !ok {
			return false
		}
		condN, ok := condVal.(float64)
		if !ok {
			return false
		}
		return valN <= condN

	case aql.Condition_IN:
		found := false
		condL, ok := condVal.([]interface{})
		if !ok {
			return false
		}
		for _, v := range condL {
			if reflect.DeepEqual(val, v) {
				found = true
			}
		}
		return found

	default:
		return false
	}
}

func matchesWhereExpression(trav *gdbi.Traveler, stmt *aql.WhereExpression) bool {
	switch stmt.Expression.(type) {
	case *aql.WhereExpression_Condition:
		cond := stmt.GetCondition()
		return matchesCondition(trav, cond)

	case *aql.WhereExpression_And:
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

	case *aql.WhereExpression_Or:
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

	case *aql.WhereExpression_Not:
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
		var i int64
		for range in {
			i++
		}
		out <- &gdbi.Traveler{Count: i}
	}()
	return context.WithValue(ctx, propLoad, false)
}

////////////////////////////////////////////////////////////////////////////////

// Limit limits incoming values to count
type Limit struct {
	count int64
}

// Process runs Limit
func (l *Limit) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i int64
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

// Fold runs Javascript fold function
type Fold struct {
	fold    *aql.FoldStatement
	imports []string
}

// Process runs fold
func (f *Fold) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		mfunc, err := jsengine.NewJSEngine(f.fold.Source, f.imports)
		if err != nil || mfunc == nil {
			log.Printf("Script Error: %s", err)
			return
		}
		s := f.fold.Init.Kind.(*structpb.Value_StructValue)
		foldValue := protoutil.AsMap(s.StructValue)
		for i := range in {
			foldValue, err = mfunc.CallDict(foldValue, i.GetCurrent().Data)
			if err != nil {
				log.Printf("Call error: %s", err)
			}
		}
		if foldValue != nil {
			i := gdbi.Traveler{}
			a := i.AddCurrent(&gdbi.DataElement{Value: foldValue})
			out <- a
		}
	}()
	return context.WithValue(ctx, propLoad, true)
}

////////////////////////////////////////////////////////////////////////////////

// GroupCount does a groupcount
type GroupCount struct {
	key string
}

func (g *GroupCount) countIDs(in gdbi.InPipe, counts map[string]int64) {
	for t := range in {
		counts[t.GetCurrent().ID]++
	}
}

func (g *GroupCount) countValues(in gdbi.InPipe, counts map[string]int64) {
	for t := range in {
		if t.GetCurrent().Data == nil {
			continue
		}
		if vi, ok := t.GetCurrent().Data[g.key]; ok {
			s := fmt.Sprintf("%v", vi)
			counts[s]++
		}
	}
}

// Process runs GroupCount
func (g *GroupCount) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		counts := map[string]int64{}
		if g.key != "" {
			g.countValues(in, counts)
		} else {
			g.countIDs(in, counts)
		}

		eo := &gdbi.Traveler{
			GroupCounts: counts,
		}
		out <- eo
	}()
	return context.WithValue(ctx, propLoad, true)
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
				s[i] = []byte(fmt.Sprintf("%#v", jsonpath.TravelerPathLookup(t, v)))
			}
			k := bytes.Join(s, []byte{0x00})
			if !kv.HasKey(k) {
				kv.Set(k, []byte{0x01})
				out <- t
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

type selectOne struct {
	mark string
}

func (s *selectOne) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			c := t.GetMark(s.mark)
			out <- t.AddCurrent(c)
		}
	}()
	return context.WithValue(ctx, propLoad, true)
}

type selectMany struct {
	marks []string
}

func (s *selectMany) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			row := make([]gdbi.DataElement, 0, len(s.marks))
			for _, mark := range s.marks {
				// TODO handle missing mark? rely on compiler to check this?
				t := t.GetMark(mark)
				if t != nil {
					row = append(row, *t)
				} else {
					row = append(row, gdbi.DataElement{})
				}
			}
			out <- t.AddCurrent(&gdbi.DataElement{Row: row})
		}
	}()
	return context.WithValue(ctx, propLoad, true)
}

type concat []gdbi.Processor

func (c concat) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		chanIn := make([]chan *gdbi.Traveler, len(c))
		chanOut := make([]chan *gdbi.Traveler, len(c))
		for i := range c {
			chanIn[i] = make(chan *gdbi.Traveler, 1000)
			chanOut[i] = make(chan *gdbi.Traveler, 1000)
		}

		for i, p := range c {
			ctx = p.Process(ctx, man, chanIn[i], chanOut[i])
		}

		wg := sync.WaitGroup{}
		wg.Add(len(c))
		for i := range c {
			go func(i int) {
				for c := range chanOut[i] {
					out <- c
				}
				wg.Done()
			}(i)
		}

		for t := range in {
			for _, ch := range chanIn {
				ch <- t
			}
		}
		for _, ch := range chanIn {
			close(ch)
		}
		wg.Wait()
	}()
	return ctx
}

/*

func mapPipe() {
	mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
	if err != nil {
		log.Printf("Script Error: %s", err)
	}

	for i := range pipe.Travelers {
		out := mfunc.Call(i.GetCurrent())
		if out != nil {
			a := i.AddCurrent(*out)
			o <- a
		}
	}
}

func foldPipe() {
	mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
	if err != nil {
		log.Printf("Script Error: %s", err)
	}

	var last *aql.QueryResult
	first := true
	for i := range pipe.Travelers {
		if first {
			last = i.GetCurrent()
			first = false
		} else {
			last = mfunc.Call(last, i.GetCurrent())
		}
	}
	if last != nil {
		i := Traveler{}
		a := i.AddCurrent(*last)
		o <- a
	}
}

func filterPipe() {
	mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
	if err != nil {
		log.Printf("Script Error: %s", err)
	}
	for i := range pipe.Travelers {
		out := mfunc.CallBool(i.GetCurrent())
		if out {
			o <- i
		}
	}
}

func filterValuesPipe() {
  // TODO only create JS engine once?
	mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
	if err != nil {
		log.Printf("Script Error: %s", err)
	}
	for i := range pipe.Travelers {
		out := mfunc.CallValueMapBool(i.State)
		if out {
			o <- i
		}
	}
}

func vertexFromValuesPipe() {
	mfunc, err := jsengine.NewJSEngine(source, pengine.imports)
	if err != nil {
		log.Printf("Script Error: %s", err)
	}
	for i := range pipe.Travelers {

		t.startTimer("javascript")
		out := mfunc.CallValueToVertex(i.State)
		t.endTimer("javascript")

		for _, j := range out {
			v := db.GetVertex(j, load)
			if v != nil {
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
			}
		}
	}
}

*/
