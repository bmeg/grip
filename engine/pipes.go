package engine

import (
  "github.com/bmeg/arachne/aql"
)

type pipe interface {
  Process(in inCh, out outCh)
}

type inCh <-chan *Element
type outCh chan<- *Element

type Element struct {
  ID string
  Label string
  Type ElementType
  Data map[string]interface{}
}

type ElementType uint8
const (
  None ElementType = iota
  Vertex
  Edge
  Count
)

type direction int
const (
  in direction = iota
  out
  both
)

type DB interface {
}

type lookup struct {
  db DB
  ids []string
  labels []string
  load bool
  elType ElementType
}
func (l *lookup) Process(in inCh, out outCh) {
  defer close(out)
  l.db.
}

type lookupAdj struct {
  db DB
  dir direction
  labels []string
}
func (l *lookupAdj) Process(in inCh, out outCh) {
}

type lookupEnd struct {
  db DB
  dir direction
  labels []string
}
func (l *lookupEnd) Process(in inCh, out outCh) {
}

type hasData struct {
  stmt *aql.HasStatement
}
func (h *hasData) Process(in inCh, out outCh) {
  defer close(out)
  for el := range in {
		if p, ok := el.Data[h.stmt.Key]; ok {
      if s, ok := p.(string); ok && contains(h.stmt.Within, s) {
        out <- el
      }
    }
  }
}

type hasLabel struct {
  labels []string
}
func (h *hasLabel) Process(in inCh, out outCh) {
  defer close(out)
  for el := range in {
    if contains(h.labels, el.Label) {
      out <- el
    }
  }
}

type hasID struct {
  ids []string
}
func (h *hasID) Process(in inCh, out outCh) {
  defer close(out)
  for el := range in {
    if contains(h.ids, el.ID) {
      out <- el
    }
  }
}

type count struct {}
func (c *count) Process(in inCh, out outCh) {
  defer close(out)
  var i int64
  for range in {
    i++
  }
  out <- &Element{
    Type: Count,
    Data: map[string]interface{}{
      "count": i,
    },
  }
}

type limit struct {
  count int64
}
func (l *limit) Process(in inCh, out outCh) {
  defer close(out)
  var i int64
  for el := range in {
    if i == l.count {
      return
    }
    out <- el
    i++
  }
}

type chain struct {
  pipes []pipe
  buffer int
}
func (c chain) Process(in inCh, out outCh) {
  if len(c.pipes) == 0 {
    return
  }
  if len(c.pipes) == 1 {
    c.pipes[0].Process(in, out)
    return
  }
  buffer := 100
  if c.buffer != 0 {
    buffer = c.buffer
  }
  mid := make(chan *Element, buffer)
  for _, pipe := range c.pipes[:len(c.pipes)-1] {
    go pipe.Process(in, mid)
  }
  c.pipes[len(c.pipes)-1].Process(mid, out)
}

/*
type groupCount struct {
  key string
}
func (g *groupCount) Process(in inCh, out outCh) {
  counts := map[string]int64{}
  for el := range in {
    if _, ok := el.Data[g.key]; ok {
      
    }
  }
  out <- &Element{
    Type: GroupCount,
    Data: counts,
  }
}

func groupCountPipe() {
	for i := range pipe.Travelers {
		var props *structpb.Struct

		if props != nil {
			if x, ok := props.Fields[label]; ok {
				groupCount[x.GetStringValue()]++ //BUG: Only supports string data
			}
		}
	}

	out := structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range groupCount {
		out.Fields[k] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(v)}}
	}

	c := Traveler{}
	o <- c.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Struct{Struct: &out}})
}
*/

/*
func valuesPipe() {
	for i := range pipe.Travelers {
		var props *structpb.Struct

		if v := i.GetCurrent().GetVertex(); v != nil && v.Data != nil {
			props = v.GetData()
		} else if v := i.GetCurrent().GetEdge(); v != nil && v.Data != nil {
			props = v.GetData()
		}

		if props != nil {
			out := structpb.Struct{Fields: map[string]*structpb.Value{}}
			if len(labels) == 0 {
				protoutil.CopyStructToStruct(&out, props)
			} else {
				protoutil.CopyStructToStructSub(&out, labels, props)
			}
			o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Struct{Struct: &out}})
		}
	}
}

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


func asPipe() {
	for i := range pipe.Travelers {
		if i.HasLabeled(label) {
			c := i.GetLabeled(label)
			o <- i.AddCurrent(*c)
		} else {
			o <- i.AddLabeled(label, *i.GetCurrent())
		}
	}
}

func matchPipe() {
	pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
	for _, matchStep := range matches {
		pipe = (*matchStep).Chain(ctx, pipe)
	}
}
*/
func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}
