package core

import (
  "fmt"
  "context"
  "testing"
  "strconv"
  "github.com/bmeg/grip/gdbi"
)

func TestOptStream(t *testing.T) {
  req := make(chan gdbi.ElementLookup, 10)
  go func() {
    defer close(req)
    for i := 0; i < 100; i++ {
      req <- gdbi.ElementLookup{
        ID:  fmt.Sprintf("%d", i),
        Ref: &gdbi.Traveler{Path:[]gdbi.DataElementID{gdbi.DataElementID{Vertex:fmt.Sprintf("to:%d", i)}}},
      }
    }
  }()

  lookup := func(ctx context.Context, req chan gdbi.ElementLookup, load bool, labels []string) chan gdbi.ElementLookup {
    out := make(chan gdbi.ElementLookup, 10)
    go func() {
      defer close(out)
      for i := range req {
        n, _ := strconv.Atoi(i.ID)
        if n % 5 != 0 && n % 3 != 0 {
          i.Vertex = &gdbi.Vertex{ID:i.ID, Label:"Vertex"}
          out <- i
        }
      }
    }()
    return out
  }

  count := 0
  for o := range OptWrapCall(context.Background(), req, true, []string{}, lookup) {
    n, _ := strconv.Atoi(o.ID)
    if n % 5 != 0 && n % 3 != 0 {
      if o.Vertex == nil {
        t.Errorf("Vertex return expected")
      }
    } else {
      if o.Vertex != nil {
        t.Errorf("Vertex return unexpected")
      }
    }
    count++
  }
  if count != 100 {
    t.Errorf("Incorrect number of items returned %d != %d", count, 100)
  }
}
