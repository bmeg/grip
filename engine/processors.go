package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	"sync"
)

type processor interface {
	process(in inPipe, out outPipe)
}

type lookupVerts struct {
	db     gdbi.GraphDB
	ids    []string
	labels []string
}

func (l *lookupVerts) process(in inPipe, out outPipe) {
	for t := range in {
		for v := range l.db.GetVertexList(context.Background(), true) {
			// TODO maybe don't bother copying the data
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: vertexData,
			}
		}
	}
}

type lookupEdges struct {
	db     gdbi.GraphDB
	ids    []string
	labels []string
}

func (l *lookupEdges) process(in inPipe, out outPipe) {
	for t := range in {
		for v := range l.db.GetEdgeList(context.Background(), true) {
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				from:     v.From,
				to:       v.To,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: edgeData,
			}
		}
	}
}

type lookupAdjOut struct {
	db     gdbi.GraphDB
	labels []string
}

func (l *lookupAdjOut) process(in inPipe, out outPipe) {
	ctx := context.Background()
	for t := range in {
		for v := range l.db.GetOutList(ctx, t.id, true, l.labels) {
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: vertexData,
			}
		}
	}
}

type lookupAdjIn struct {
	db     gdbi.GraphDB
	labels []string
}

func (l *lookupAdjIn) process(in inPipe, out outPipe) {
	ctx := context.Background()
	for t := range in {
		for v := range l.db.GetInList(ctx, t.id, true, l.labels) {
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: vertexData,
			}
		}
	}
}

type inEdge struct {
	db     gdbi.GraphDB
	labels []string
}

func (i *inEdge) process(in inPipe, out outPipe) {
	ctx := context.Background()
	for t := range in {
		for v := range i.db.GetInEdgeList(ctx, t.id, true, i.labels) {
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				from:     v.From,
				to:       v.To,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: edgeData,
			}
		}
	}
}

type outEdge struct {
	db     gdbi.GraphDB
	labels []string
}

func (o *outEdge) process(in inPipe, out outPipe) {
	ctx := context.Background()
	for t := range in {
		for v := range o.db.GetOutEdgeList(ctx, t.id, true, o.labels) {
			out <- &traveler{
				id:       v.Gid,
				label:    v.Label,
				from:     v.From,
				to:       v.To,
				marks:    t.marks,
				data:     protoutil.AsMap(v.Data),
				dataType: edgeData,
			}
		}
	}
}

type values struct {
	keys []string
}

func (v *values) process(in inPipe, out outPipe) {
	for t := range in {
		if t.data == nil {
			continue
		}

		if len(v.keys) == 0 {
			out <- &traveler{
				marks:    t.marks,
				value:    t.data,
				dataType: valueData,
			}
			continue
		}

		for _, key := range v.keys {
			if z, ok := t.data[key]; ok {
				out <- &traveler{
					marks:    t.marks,
					value:    z,
					dataType: valueData,
				}
			}
		}
	}
}

type hasData struct {
	stmt *aql.HasStatement
}

func (h *hasData) process(in inPipe, out outPipe) {
	for t := range in {
		if t.data == nil {
			continue
		}
		if z, ok := t.data[h.stmt.Key]; ok {
			if s, ok := z.(string); ok && contains(h.stmt.Within, s) {
				out <- t
			}
		}
	}
}

type hasLabel struct {
	labels []string
}

func (h *hasLabel) process(in inPipe, out outPipe) {
	for t := range in {
		if contains(h.labels, t.label) {
			out <- t
		}
	}
}

type hasID struct {
	ids []string
}

func (h *hasID) process(in inPipe, out outPipe) {
	for t := range in {
		if contains(h.ids, t.id) {
			out <- t
		}
	}
}

type count struct{}

func (c *count) process(in inPipe, out outPipe) {
	var i int64
	for range in {
		i++
	}
	out <- &traveler{
		dataType: countData,
		count:    i,
	}
}

type limit struct {
	count int64
}

func (l *limit) process(in inPipe, out outPipe) {
	var i int64
	for t := range in {
		if i == l.count {
			return
		}
		out <- t
		i++
	}
}

type groupCount struct {
	key string
}

// TODO except, if you select.by("name") this is counting by value, not ID
func (g *groupCount) countIDs(in inPipe, counts map[string]int64) {
	for t := range in {
		counts[t.id]++
	}
}

func (g *groupCount) countValues(in inPipe, counts map[string]int64) {
	for t := range in {
		if t.data == nil {
			continue
		}
		if vi, ok := t.data[g.key]; ok {
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

func (g *groupCount) process(in inPipe, out outPipe) {
	counts := map[string]int64{}

	if g.key != "" {
		g.countValues(in, counts)
	} else {
		g.countIDs(in, counts)
	}

	eo := &traveler{
		dataType:    groupCountData,
		groupCounts: counts,
	}
	out <- eo
}

type marker struct {
	marks []string
}

func (m *marker) process(in inPipe, out outPipe) {
	for t := range in {
		// Processors are not synchronized; they are independent, concurrent, and buffered.
		// Marks must be copied when written, so that a downstream processor is guaranteed
		// a consistent view of the marks.
		marks := t.marks
		t.marks = map[string]*traveler{}
		// copy the existing marks
		for k, v := range marks {
			t.marks[k] = v
		}
		// add the new marks
		for _, k := range m.marks {
			t.marks[k] = t
		}
		out <- t
	}
}

type selectOne struct {
	mark string
}

func (s *selectOne) process(in inPipe, out outPipe) {
	for t := range in {
		x := t.marks[s.mark]
		out <- x
	}
}

type selectMany struct {
	marks []string
}

func (s *selectMany) process(in inPipe, out outPipe) {
	for t := range in {
		row := make([]*traveler, len(s.marks))
		for _, mark := range s.marks {
			// TODO handle missing mark? rely on compiler to check this?
			row = append(row, t.marks[mark])
		}
		out <- &traveler{
			dataType: rowData,
			row:      row,
		}
	}
}

type concat []processor

func (c concat) process(in inPipe, out outPipe) {
	chans := make([]chan *traveler, len(c))
	for i := range c {
		chans[i] = make(chan *traveler)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(c))

	for i, p := range c {
		go func(i int) {
			p.process(chans[i], out)
			wg.Done()
		}(i)
	}

	for t := range in {
		for _, ch := range chans {
			ch <- t
		}
	}
	wg.Done()
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
