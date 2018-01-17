package rocksdb

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/rocksdb"
	"context"
	"os"
	"testing"
	"log"
)


func graphEngine(path string) gdbi.ArachneInterface {
	r, err := rocksdb.RocksBuilder(path)
	if err != nil {
		log.Printf("Failure setting up DB")
	}
	return kvgraph.NewKVGraph(r)
}


func BenchmarkSetVertex(b *testing.B) {
	db := graphEngine("bench_test.db")
	db.AddGraph("test")
	graph := db.Graph("test")
	for i := 0; i < b.N; i++ {
		graph.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	db.Close()
	os.RemoveAll("bench_test.db")
}


func TestSetVertex(b *testing.T) {
	db := graphEngine("bench_test.db")
	db.AddGraph("test")
	graph := db.Graph("test")
	for i := 0; i < 100; i++ {
		graph.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}

	for i := range graph.Query().V([]string{}).Count().Execute(context.Background()) {
		log.Printf("Found: %s", i)
	}
	db.Close()
	os.RemoveAll("bench_test.db")
}


func BenchmarkSetE(b *testing.B) {
	db := graphEngine("bench_test.db")
	db.AddGraph("test")
	graph := db.Graph("test")
	for i := 0; i < b.N; i++ {
		graph.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	for i := 0; i < b.N-1; i++ {
		graph.SetEdge(aql.Edge{From: fmt.Sprintf("%d", i), To: fmt.Sprintf("%d", i+1)})
	}

	db.Close()
	os.RemoveAll("bench_test.db")
}


func TestSetE(b *testing.T) {
	db := graphEngine("bench_test.db")
	db.AddGraph("test")
	graph := db.Graph("test")
	for i := 0; i < 100; i++ {
		graph.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	for i := 0; i < 99; i++ {
		graph.SetEdge(aql.Edge{From: fmt.Sprintf("%d", i), To: fmt.Sprintf("%d", i+1)})
	}
	for i := range graph.Query().E().Count().Execute(context.Background()) {
		log.Printf("Found: %s", i)
	}
	db.Close()
	os.RemoveAll("bench_test.db")
}

/*
func TestAddRemove(t *testing.T) {
	db := graphEngine("bench_test.db")
	db.AddGraph("test")
	graph := db.Graph("test")

	for i := 0; i < 100; i++ {
		graph.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	for i := 0; i < 100-1; i++ {
		graph.SetEdge(aql.Edge{From: fmt.Sprintf("%d", i), To: fmt.Sprintf("%d", i+1), Label:"test"})
	}

	for i := 0; i < 100; i += 2 {
		db.Query("test").V(fmt.Sprintf("%d", i)).Drop().Run()
	}
	res, _ := db.Query().V().Count().First()
	if res.Value.GetIntValue() != 50 {
		t.Error("Wrong vertex count")
	}

	res, _ = db.Query().E().Count().First()
	if res.Value.GetIntValue() != 0 {
		t.Error("Wrong edge count")
	}



	db.Close()
	os.RemoveAll("bench_test.db")
}

*/
