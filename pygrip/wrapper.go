package main

/*
#include <stdint.h> // for uintptr_t
*/

import "C"

import (
	"context"
	"encoding/json"

	"runtime/cgo"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/leveldb"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/encoding/protojson"
)

var graphDB gdbi.GraphDB

type GraphHandle uintptr
type QueryReaderHandle uintptr

type Reader interface {
	Done() bool
	Next() string
}

//export NewMemServer
func NewMemServer() GraphHandle {
	db, _ := leveldb.NewMemKVInterface("", kvi.Options{})
	graphDB = kvgraph.NewKVGraph(db)
	err := graphDB.AddGraph("default")
	if err != nil {
		log.Errorf("Graph init error: %s\n", err)
	}
	g, err := graphDB.Graph("default")
	if err != nil {
		log.Errorf("Graph init error: %s\n", err)
	}
	return GraphHandle(cgo.NewHandle(g))
}

func CloseServer(graph GraphHandle) {
	cgo.Handle(graph).Delete()
}

//export AddVertex
func AddVertex(graph GraphHandle, gid, label, jdata string) {
	data := map[string]any{}
	err := json.Unmarshal([]byte(jdata), &data)
	if err != nil {
		log.Errorf("Data error: %s : %s\n", err, jdata)
	}

	g := cgo.Handle(graph).Value().(gdbi.GraphInterface)

	g.AddVertex([]*gdbi.Vertex{
		{ID: gid, Label: label, Data: data},
	})
}

//export AddEdge
func AddEdge(graph GraphHandle, gid, src, dst, label, jdata string) {
	data := map[string]any{}
	err := json.Unmarshal([]byte(jdata), &data)
	if err != nil {
		log.Errorf("Data error: %s : %s\n", err, jdata)
	}

	g := cgo.Handle(graph).Value().(gdbi.GraphInterface)

	g.AddEdge([]*gdbi.Edge{
		{ID: gid, To: dst, From: src, Label: label, Data: data},
	})
}

type QueryReader struct {
	pipe    gdbi.Pipeline
	results chan *gripql.QueryResult
	current *gripql.QueryResult
}

//export Query
func Query(graph GraphHandle, jquery string) QueryReaderHandle {
	query := gripql.GraphQuery{}
	err := protojson.Unmarshal([]byte(jquery), &query)
	if err != nil {
		log.Errorf("Query error: %s : %s\n", err)
	}

	g := cgo.Handle(graph).Value().(gdbi.GraphInterface)
	compiler := core.NewCompiler(g)
	pipe, err := compiler.Compile(query.Query, nil)
	if err != nil {
		log.Errorf("Compile error: %s : %s\n", err)
	}

	ctx := context.Background()

	bufsize := 5000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		graph := pipe.Graph()
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager("./") //TODO: in memory option
		rPipe := pipeline.Start(ctx, pipe, man, bufsize, nil, nil)
		for t := range rPipe.Outputs {
			if !t.IsSignal() {
				resch <- pipeline.Convert(graph, dataType, markTypes, t)
			}
		}
		man.Cleanup()
	}()
	var o = &QueryReader{
		pipe:    pipe,
		results: resch,
		current: nil,
	}
	return QueryReaderHandle(cgo.NewHandle(o))
}

//export ReaderDone
func ReaderDone(reader QueryReaderHandle) bool {
	r := cgo.Handle(reader).Value().(*QueryReader)
	return r.Done()
}

//export ReaderNext
func ReaderNext(reader QueryReaderHandle) *C.char {
	r := cgo.Handle(reader).Value().(*QueryReader)
	o := r.Next()
	return C.CString(o)
}

func (r *QueryReader) Next() string {
	out, _ := protojson.Marshal(r.current)
	return string(out)
}

func (r *QueryReader) Done() bool {
	select {
	case i, ok := <-r.results:
		if ok {
			r.current = i
			return false
		}
		return true
	}
}

func main() {}
