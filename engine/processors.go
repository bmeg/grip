package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	//"log"
	"sync"
)

type LookupVerts struct {
	db     gdbi.GraphInterface
	ids    []string
	labels []string
}

func (l *LookupVerts) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		if len(l.ids) == 0 {
			for v := range l.db.GetVertexList(context.Background(), true) {
				out <- t.AddCurrent(&gdbi.DataElement{
					Id:    v.Gid,
					Label: v.Label,
					Data:  protoutil.AsMap(v.Data),
				})
			}
		} else {
			for _, i := range l.ids {
				v := l.db.GetVertex(i, true)
				if v != nil {
					out <- t.AddCurrent(&gdbi.DataElement{
						Id:    v.Gid,
						Label: v.Label,
						Data:  protoutil.AsMap(v.Data),
					})
				}
			}
		}
	}
}

type LookupEdges struct {
	db     gdbi.GraphInterface
	ids    []string
	labels []string
}

func (l *LookupEdges) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		for v := range l.db.GetEdgeList(context.Background(), true) {
			out <- t.AddCurrent(&gdbi.DataElement{
				Id:    v.Gid,
				Label: v.Label,
				From:  v.From,
				To:    v.To,
				Data:  protoutil.AsMap(v.Data),
			})
		}
	}
}

type LookupVertexAdjOut struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *LookupVertexAdjOut) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().Id,
				Ref: i,
			}
		}
	}()
	for ov := range l.db.GetOutChannel(queryChan, true, l.labels) {
		i := ov.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    ov.Vertex.Gid,
			Label: ov.Vertex.Label,
			Data:  protoutil.AsMap(ov.Vertex.Data),
		})
	}
}

type LookupEdgeAdjOut struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *LookupEdgeAdjOut) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().To,
				Ref: &i,
			}
		}
	}()
	for v := range l.db.GetVertexChannel(queryChan, true) {
		i := v.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    v.Vertex.Gid,
			Label: v.Vertex.Label,
			Data:  protoutil.AsMap(v.Vertex.Data),
		})
	}
}

type LookupVertexAdjIn struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *LookupVertexAdjIn) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().Id,
				Ref: i,
			}
		}
	}()
	for v := range l.db.GetInChannel(queryChan, true, l.labels) {
		i := v.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    v.Vertex.Gid,
			Label: v.Vertex.Label,
			Data:  protoutil.AsMap(v.Vertex.Data),
		})
	}
}

type LookupEdgeAdjIn struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *LookupEdgeAdjIn) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().From,
				Ref: &i,
			}
		}
	}()
	for v := range l.db.GetVertexChannel(queryChan, true) {
		i := v.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    v.Vertex.Gid,
			Label: v.Vertex.Label,
			Data:  protoutil.AsMap(v.Vertex.Data),
		})
	}
}

type InEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *InEdge) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().Id,
				Ref: i,
			}
		}
	}()

	for v := range l.db.GetInEdgeChannel(queryChan, true, l.labels) {
		i := v.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    v.Edge.Gid,
			To:    v.Edge.To,
			From:  v.Edge.From,
			Label: v.Edge.Label,
			Data:  protoutil.AsMap(v.Edge.Data),
		})
	}
}

type OutEdge struct {
	db     gdbi.GraphInterface
	labels []string
}

func (l *OutEdge) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- gdbi.ElementLookup{
				ID:  i.GetCurrent().Id,
				Ref: i,
			}
		}
	}()

	for v := range l.db.GetOutEdgeChannel(queryChan, true, l.labels) {
		i := v.Ref.(*gdbi.Traveler)
		out <- i.AddCurrent(&gdbi.DataElement{
			Id:    v.Edge.Gid,
			To:    v.Edge.To,
			From:  v.Edge.From,
			Label: v.Edge.Label,
			Data:  protoutil.AsMap(v.Edge.Data),
		})
	}

}

type Values struct {
	keys []string
}

func (v *Values) Process(in gdbi.InPipe, out gdbi.OutPipe) {
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
}

type HasData struct {
	stmt *aql.HasStatement
}

func (h *HasData) Process(in gdbi.InPipe, out gdbi.OutPipe) {
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
}

type HasLabel struct {
	labels []string
}

func (h *HasLabel) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		if contains(h.labels, t.GetCurrent().Label) {
			out <- t
		}
	}
}

type HasID struct {
	ids []string
}

func (h *HasID) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		if contains(h.ids, t.GetCurrent().Id) {
			out <- t
		}
	}
}

type Count struct{}

func (c *Count) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	var i int64
	for range in {
		i++
	}
	out <- &gdbi.Traveler{Count: i}
}

type Limit struct {
	count int64
}

func (l *Limit) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	var i int64
	for t := range in {
		if i == l.count {
			return
		}
		out <- t
		i++
	}
}

type GroupCount struct {
	key string
}

// TODO except, if you select.by("name") this is counting by value, not ID
func (g *GroupCount) countIDs(in gdbi.InPipe, counts map[string]int64) {
	for t := range in {
		counts[t.GetCurrent().Id]++
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

func (g *GroupCount) Process(in gdbi.InPipe, out gdbi.OutPipe) {
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
}

type Marker struct {
	mark string
}

func (m *Marker) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		out <- t.AddMark(m.mark, t.GetCurrent())
	}
}

type selectOne struct {
	mark string
}

func (s *selectOne) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		c := t.GetMark(s.mark)
		out <- t.AddCurrent(c)
	}
}

type selectMany struct {
	marks []string
}

func (s *selectMany) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	for t := range in {
		row := make([]gdbi.DataElement, 0, len(s.marks))
		for _, mark := range s.marks {
			// TODO handle missing mark? rely on compiler to check this?
			row = append(row, *t.GetMark(mark))
		}
		out <- t.AddCurrent(&gdbi.DataElement{Row: row})
	}
}

type concat []gdbi.Processor

func (c concat) Process(in gdbi.InPipe, out gdbi.OutPipe) {
	chans := make([]chan *gdbi.Traveler, len(c))
	for i := range c {
		chans[i] = make(chan *gdbi.Traveler)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(c))

	for i, p := range c {
		go func(i int, p gdbi.Processor) {
			p.Process(chans[i], out)
			wg.Done()
		}(i, p)
	}

	for t := range in {
		for _, ch := range chans {
			ch <- t
		}
	}
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
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
