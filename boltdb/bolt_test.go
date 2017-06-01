package boltdb

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"os"
	"testing"
)

func BenchmarkSetVertex(b *testing.B) {
	db := NewBoltArachne("bench_test.db")
	for i := 0; i < b.N; i++ {
		db.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	db.Close()
	os.Remove("bench_test.db")
}

func BenchmarkAddV(b *testing.B) {
	db := NewBoltArachne("bench_test.db")
	for i := 0; i < b.N; i++ {
		db.Query().AddV(fmt.Sprintf("%d", i)).Run()
	}
	db.Close()
	os.Remove("bench_test.db")
}

func BenchmarkAddE(b *testing.B) {
	db := NewBoltArachne("bench_test.db")
	for i := 0; i < b.N; i++ {
		db.Query().AddV(fmt.Sprintf("%d", i)).Run()
	}
	for i := 0; i < b.N-1; i++ {
		db.Query().V(fmt.Sprintf("%d", i)).AddE("test").To(fmt.Sprintf("%d", i+1)).Run()
	}

	db.Close()
	os.Remove("bench_test.db")
}

func BenchmarkSetE(b *testing.B) {
	db := NewBoltArachne("bench_test.db")
	for i := 0; i < b.N; i++ {
		db.SetVertex(aql.Vertex{Gid: fmt.Sprintf("%d", i)})
	}
	for i := 0; i < b.N-1; i++ {
		db.SetEdge(aql.Edge{Out: fmt.Sprintf("%d", i), In: fmt.Sprintf("%d", i+1)})
	}

	db.Close()
	os.Remove("bench_test.db")
}
