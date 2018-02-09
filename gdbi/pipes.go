package gdbi

// TODO with a query planner, "load" could instead be a list of property keys

func vertexIDLookup(ctx context.Context, db DBI, ids []string, load bool, out chan<- *aql.Vertex) {
  db.GetVertices(ctx, ids, load, out)
}

func vertexLabelLookup(ctx context.Context, db DBI, labels []string, load bool, out chan<- *aql.Vertex) {
  db.GetVerticesByLabel(ctx, labels, load, out)
}

func vertexList(ctx context.Context, db DBI, load bool, out chan<- *aql.Vertex) {
  db.GetVertexList(ctx, load, out)
}

func edgeList(ctx context.Context, db DBI, load bool, out chan<- *aql.Edge) {
  db.GetEdgeList(ctx, load, out)
}

func vertexHasIDPipe(ids []string, in <-chan *aql.Vertex, out chan<- *aql.Vertex) {
  for v := range input {
    if contains(ids, v.Gid) {
      out <- v
    }
  }
}

func edgeHasIDPipe(ids []string, in <-chan *aql.Edge, out chan<- *aql.Edge) {
  for e := range input {
    if contains(ids, e.Gid) {
      out <- e
    }
  }
}

func vertexHasLabelPipe(labels []string, in <-chan *aql.Vertex, out chan<- *aql.Vertex) {
  for v := range input {
    if contains(ids, v.Label) {
      out <- v
    }
  }
}

func edgeHasLabelPipe(labels []string, in <-chan *aql.Edge, out chan<- *aql.Edge) {
  for e := range input {
    if contains(ids, e.Label) {
      out <- e
    }
  }
}
// TODO make query compiler that understands that some operations could be constructed
//      into a single database query

func vertexHasPropPipe(key string, vals) {
  // convert values to strings for comparison
  svals := make([]string, len(vals))
  for _, val := range vals {
    svals = append(svals, val.GetStringValue())
  }

  for v := range in {
		if p, ok := v.Data.Fields[key]; ok && contains(svals, p) {
      out <- v
    }
  }
}


func vertexOutLookup(ctx, db, ids, labels, load, out) {
  db.GetOutList(ctx, ids, load, labels, out)
}

func vertexInLookup(ctx, db, ids, labels, load, out) {
  db.GetInList(ctx, ids, load, labels, out)
}

func outPipe() {
	if pipe.State == StateVertexList || pipe.State == StateRawVertexList {

	} else if pipe.State == StateEdgeList || pipe.State == StateRawEdgeList {

		idList := make(chan string, 100)
		travelerList := make(chan Traveler, 100)

		go func() {
			defer close(idList)
			defer close(travelerList)
			for i := range pipe.Travelers {
				e := i.GetCurrent().GetEdge()
				idList <- e.To
				travelerList <- i
			}
		}()

		for v := range db.GetVertexListByID(ctx, idList, load) {
			i := <-travelerList
			if v != nil {
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
			}
		}

	} else {
		log.Printf("Weird State: %d", pipe.State)
	}
}

func inPipe(db, ctx, input, output) {

	if pipe.State == StateVertexList || pipe.State == StateRawVertexList {

	} else if pipe.State == StateEdgeList || pipe.State == StateRawEdgeList {
		for i := range pipe.Travelers {
			if e := i.GetCurrent().GetEdge(); e != nil {
				v := db.GetVertex(e.From, load)
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Vertex{Vertex: v}})
			}
		}
	}
}

func outEPipe() {
	for i := range pipe.Travelers {
		if v := i.GetCurrent().GetVertex(); v != nil {
			for oe := range db.GetOutEdgeList(ctx, v.Gid, load, key) {
				le := oe
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
			}
		}
	}
}

func bothEPipe() {
	for i := range pipe.Travelers {
		if v := i.GetCurrent().GetVertex(); v != nil {

			for oe := range db.GetOutEdgeList(ctx, v.Gid, load, key) {
				le := oe
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
			}

			for oe := range db.GetInEdgeList(ctx, v.Gid, load, key) {
				le := oe
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &le}})
			}
		}
	}
}

func outBundlePipe() {
	for i := range pipe.Travelers {
		if v := i.GetCurrent().GetVertex(); v != nil {
			//log.Printf("GetEdgeList: %s", v.Gid)
			for oe := range db.GetOutBundleList(ctx, v.Gid, load, key) {
				le := oe
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Bundle{Bundle: &le}})
			}
			//log.Printf("Done GetEdgeList: %s", v.Gid)
		}
	}
}

func inEPipe() {
	for i := range pipe.Travelers {
		if v := i.GetCurrent().GetVertex(); v != nil {
			for e := range db.GetInEdgeList(ctx, v.Gid, load, key) {
				el := e
				o <- i.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_Edge{Edge: &el}})
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

func groupCountPipe() {
	groupCount := map[string]int{}
	for i := range pipe.Travelers {
		var props *structpb.Struct
		if v := i.GetCurrent().GetVertex(); v != nil && v.Data != nil {
			props = v.GetData()
		} else if v := i.GetCurrent().GetEdge(); v != nil && v.Data != nil {
			props = v.GetData()
		}
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

func countPipe() {
	var count int32
	for range pipe.Travelers {
		count++
	}
	//log.Printf("Counted: %d", count)
	trav := Traveler{}
	o <- trav.AddCurrent(aql.QueryResult{Result: &aql.QueryResult_IntValue{IntValue: count}})
}

func limitPipe() {
	var count int64
	for i := range pipe.Travelers {
		if count < limit {
			o <- i
		} else {
			cancel()
		}
		count++
	}
}

func matchPipe() {
	pipe := pengine.startPipe(context.WithValue(ctx, propLoad, true))
	for _, matchStep := range matches {
		pipe = (*matchStep).Chain(ctx, pipe)
	}
}
