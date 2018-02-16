package engine

type inPipe <-chan *traveler
type outPipe chan<- *traveler

type traveler struct {
	id          string
	label       string
	from, to    string
	data        map[string]interface{}
	marks       map[string]*traveler
	count       int64
	groupCounts map[string]int64
	row         []*traveler
	value       interface{}
	dataType
}

type dataType uint8

const (
	noData dataType = iota
	vertexData
	edgeData
	countData
	groupCountData
	valueData
	rowData
)

func start(procs []processor, bufsize int) <-chan *traveler {
	if len(procs) == 0 {
		ch := make(chan *traveler)
		close(ch)
		return ch
	}

	in := make(chan *traveler)
	final := make(chan *traveler, bufsize)

	// Write an empty traveler to input
	// to trigger the computation.
	go func() {
		in <- &traveler{}
		close(in)
	}()

	for i := 0; i < len(procs)-1; i++ {
		glue := make(chan *traveler, bufsize)
		go func(i int, in inPipe, glue outPipe) {
			procs[i].process(in, glue)
			close(glue)
		}(i, in, glue)
		in = glue
	}

	go func() {
		procs[len(procs)-1].process(in, final)
		close(final)
	}()

	return final
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

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}
