
package rocksdb

import (
  "os"
  "fmt"
  "testing"
  "github.com/bmeg/arachne/ophion"
)

func BenchmarkSetVertex(b *testing.B) {
  db := NewRocksArachne("bench_test.db")
  for i := 0; i < b.N; i++ {
    db.SetVertex(ophion.Vertex{Gid:fmt.Sprintf("%d", i)})
  }
  db.Close()
  os.RemoveAll("bench_test.db")
}


func BenchmarkAddV(b *testing.B) {
  db := NewRocksArachne("bench_test.db")
  for i := 0; i < b.N; i++ {
    db.Query().AddV(fmt.Sprintf("%d", i)).Run()
  }
  db.Close()
  os.RemoveAll("bench_test.db")
}


func BenchmarkAddE(b *testing.B) {
  db := NewRocksArachne("bench_test.db")
  for i := 0; i < b.N; i++ {
    db.Query().AddV(fmt.Sprintf("%d", i)).Run()
  }
  for i := 0; i < b.N - 1; i++ {
    db.Query().V(fmt.Sprintf("%d", i)).AddE("test").To(fmt.Sprintf("%d", i+1)).Run()
  }

  db.Close()
  os.RemoveAll("bench_test.db")
}


func BenchmarkSetE(b *testing.B) {
  db := NewRocksArachne("bench_test.db")
  for i := 0; i < b.N; i++ {
    db.SetVertex(ophion.Vertex{Gid:fmt.Sprintf("%d", i)})
  }
  for i := 0; i < b.N - 1; i++ {
    db.SetEdge( ophion.Edge{Out:fmt.Sprintf("%d", i), In:fmt.Sprintf("%d", i+1)} )
  }

  db.Close()
  os.RemoveAll("bench_test.db")
}



func TestAddRemove(t *testing.T) {
  db := NewRocksArachne("bench_test.db")
  for i := 0; i < 100; i++ {
      db.Query().AddV(fmt.Sprintf("%d", i)).Run()
  }
  for i := 0; i < 100 - 1; i++ {
    db.Query().V(fmt.Sprintf("%d", i)).AddE("test").To(fmt.Sprintf("%d", i+1)).Run()
  }

  /*
  for r := range db.Query().V().Execute() {
    fmt.Printf("PreV: %s\n", r.Value.GetVertex())
  }

  for r := range db.Query().E().Execute() {
    fmt.Printf("PreE: %s\n", r.Value.GetEdge())
  }
  */

  for i := 0; i < 100; i+=2 {
    db.Query().V(fmt.Sprintf("%d", i)).Drop().Run()
  }
  res, _ := db.Query().V().Count().First()
  if res.Value.GetIntValue() != 50 {
    t.Error("Wrong vertex count")
  }

  res, _ = db.Query().E().Count().First()
  if res.Value.GetIntValue() != 0 {
    t.Error("Wrong edge count")
  }

  /*
  for r := range db.Query().V().Execute() {
    fmt.Printf("V: %s\n", r.Value.GetVertex())
  }

  for r := range db.Query().E().Execute() {
    fmt.Printf("E: %s\n", r.Value.GetEdge())
  }
  */

  db.Close()
  os.RemoveAll("bench_test.db")
}
