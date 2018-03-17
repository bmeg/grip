package engine

import (
	"bytes"
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/jsengine"
	_ "github.com/bmeg/arachne/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/otto" // import otto so it registers with the driver map
	_ "github.com/bmeg/arachne/jsengine/v8"   // import v8 so it registers with the driver map
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"log"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
// LookupVerts starts query by looking on vertices
type LookupVerts struct {
	db  gdbi.GraphInterface
	ids []string
}

// Process LookupVerts
func (l *LookupVerts) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if len(l.ids) == 0 {
				for v := range l.db.GetVertexList(context.Background(), true) {
					out <- t.AddCurrent(&gdbi.DataElement{
						ID:    v.Gid,
						Label: v.Label,
						Data:  protoutil.AsMap(v.Data),
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetVertex(i, true)
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

// Process LookupVerts
func (l *LookupVertsIndex) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetVertexChannel(queryChan, true) {
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
	db     gdbi.GraphInterface
	ids    []string
	labels []string
}

// Process runs LookupEdges
func (l *LookupEdges) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			for v := range l.db.GetEdgeList(context.Background(), true) {
				out <- t.AddCurrent(&gdbi.DataElement{
					ID:    v.Gid,
					Label: v.Label,
					From:  v.From,
					To:    v.To,
					Data:  protoutil.AsMap(v.Data),
				})
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
func (l *LookupVertexAdjOut) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for ov := range l.db.GetOutChannel(queryChan, true, l.labels) {
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
	db     gdbi.GraphInterface
	labels []string
}

// Process runs LookupEdgeAdjOut
func (l *LookupEdgeAdjOut) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetVertexChannel(queryChan, true) {
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
	db     gdbi.GraphInterface
	labels []string
}

// Process runs LookupVertexAdjIn
func (l *LookupVertexAdjIn) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetInChannel(queryChan, true, l.labels) {
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
	db     gdbi.GraphInterface
	labels []string
}

// Process runs LookupEdgeAdjIn
func (l *LookupEdgeAdjIn) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetVertexChannel(queryChan, true) {
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
// InEdge finds the incoming edges
type InEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

// Process runs InEdge
func (l *InEdge) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetInEdgeChannel(queryChan, true, l.labels) {
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
// OutEdge finds the outgoing edges
type OutEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

// Process runs OutEdge
func (l *OutEdge) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
		for v := range l.db.GetOutEdgeChannel(queryChan, true, l.labels) {
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
// Values selects fields from current element
type Values struct {
	keys []string
}

// Process runs Values step
func (v *Values) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.GetCurrent().Data == nil {
				continue
			}
			if len(v.keys) == 0 {
				d := t.GetCurrent().Data

				data := map[string]interface{}{}
				for _, i := range v.keys {
					data[i] = d[i]
				}
				o := t.AddCurrent(&gdbi.DataElement{
					Data: data,
				})
				out <- o
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// HasData filters based on data
type HasData struct {
	stmt *aql.HasStatement
}

// Process runs HasData
func (h *HasData) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.GetCurrent().Data == nil {
				continue
			}
			if z, ok := t.GetCurrent().Data[h.stmt.Key]; ok {
				if s, ok := z.(string); ok && contains(h.stmt.Within, s) {
					out <- t
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// HasLabel filters based on label match
type HasLabel struct {
	labels []string
}

// Process runs HasLabel
func (h *HasLabel) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if contains(h.labels, t.GetCurrent().Label) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// HasID filters based on ID
type HasID struct {
	ids []string
}

// Process runs HasID
func (h *HasID) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if contains(h.ids, t.GetCurrent().ID) {
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
func (c *Count) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i int64
		for range in {
			i++
		}
		log.Printf("Done counting")
		out <- &gdbi.Traveler{Count: i}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// Limit limits incoming values to count
type Limit struct {
	count int64
}

// Process runs Limit
func (l *Limit) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
func (f *Fold) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
			a := i.AddCurrent(&gdbi.DataElement{Data: foldValue})
			out <- a
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// GroupCount does a groupcount
type GroupCount struct {
	key string
}

// TODO except, if you select.by("name") this is counting by value, not ID
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
			// TODO only counting string values.
			//      how to handle other simple types? (int, etc)
			//      what to do for objects? gremlin returns an error.
			//      how to return errors? Add Error travelerType?
			if s, ok := vi.(string); ok {
				counts[s]++
			}
		}
	}
}

// Process runs GroupCount
func (g *GroupCount) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
	return ctx
}

////////////////////////////////////////////////////////////////////////////////
// Distinct only returns district objects as defined by the set of select features
type Distinct struct {
	vals []string
}

func (g *Distinct) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		kv := man.GetTempKV()
		for t := range in {
			cur := t.GetCurrent().ToDict()
			s := make([][]byte, len(g.vals))
			for i := range g.vals {
				s[i] = []byte(jsonpath.GetString(cur, g.vals[i]))
			}
			k := bytes.Join(s, []byte{0x00})
			if !kv.HasKey(k) {
				kv.Set(k, []byte{0x01})
				out <- t
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
func (m *Marker) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			out <- t.AddMark(m.mark, t.GetCurrent())
		}
	}()
	return ctx
}

type selectOne struct {
	mark string
}

func (s *selectOne) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			c := t.GetMark(s.mark)
			out <- t.AddCurrent(c)
		}
	}()
	return ctx
}

type selectMany struct {
	marks []string
}

func (s *selectMany) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			row := make([]gdbi.DataElement, 0, len(s.marks))
			for _, mark := range s.marks {
				// TODO handle missing mark? rely on compiler to check this?
				row = append(row, *t.GetMark(mark))
			}
			out <- t.AddCurrent(&gdbi.DataElement{Row: row})
		}
	}()
	return ctx
}

type concat []Processor

func (c concat) Process(ctx context.Context, man Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		chan_in := make([]chan *gdbi.Traveler, len(c))
		chan_out := make([]chan *gdbi.Traveler, len(c))
		for i := range c {
			chan_in[i] = make(chan *gdbi.Traveler, 1000)
			chan_out[i] = make(chan *gdbi.Traveler, 1000)
		}

		for i, p := range c {
			ctx = p.Process(ctx, man, chan_in[i], chan_out[i])
		}

		wg := sync.WaitGroup{}
		wg.Add(len(c))
		for i := range c {
			go func(i int) {
				for c := range chan_out[i] {
					out <- c
				}
				wg.Done()
			}(i)
		}

		for t := range in {
			for _, ch := range chan_in {
				ch <- t
			}
		}
		for _, ch := range chan_in {
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
