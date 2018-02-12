package engine

type reader <-chan *traveler
type writer chan<- *traveler

type traveler struct {
  id string
  label string
  data map[string]interface{}
  marks map[string]*traveler
  count int64
  groupCounts map[string]int64
  row []*traveler
  dataType
}

type dataType uint8
const (
  noData dataType = iota
  vertexData
  edgeData
  countData
  groupCountData
  rowData
)

type direction int
const (
  in direction = iota
  out
  both
)

type DB interface {
}

func run(procs []processor, in reader, out writer, bufsize int) {
  if len(procs) == 0 {
    close(out)
    return
  }
  if len(procs) == 1 {
    procs[0].process(in, out)
    close(out)
    return
  }

  for i := 0; i < len(procs) - 1; i++ {
    glue := make(chan *traveler, bufsize)
    go func(i int, in reader, out writer) {
      procs[i].process(in, out)
      close(out)
    }(i, in, glue)
    in = glue
  }
  procs[len(procs)-1].process(in, out)
  close(out)
}

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
