package core

import (
  "context"
  "github.com/bmeg/grip/gdbi"
)

type EdgeFunc func(ctx context.Context, req chan gdbi.ElementLookup, load bool, labels []string) chan gdbi.ElementLookup

/*
for ov := range OptWrapCall( ctx, queryChan, l.loadData, l.labels, l.db.GetOutChannel ) {
  i := ov.Ref
  if ov.Vertex != nil {
    out <- i.AddCurrent(&gdbi.DataElement{
      ID:     ov.Vertex.ID,
      Label:  ov.Vertex.Label,
      Data:   ov.Vertex.Data,
      Loaded: ov.Vertex.Loaded,
    })
  } else {
    out <- i.AddCurrent(nil)
  }
}
*/

func OptWrapCall(ctx context.Context, req chan gdbi.ElementLookup, load bool, labels []string, f EdgeFunc) chan gdbi.ElementLookup {
  //track incoming requests
  reqIn := make(chan gdbi.ElementLookup, 10)
  order := make(chan gdbi.ElementLookup, 10)
  go func() {
    defer close(reqIn)
    defer close(order)
    for i := range req {
      reqIn <- i
      order <- i
    }
  }()
  out := make(chan gdbi.ElementLookup, 10)
  //pass requests to method
  go func() {
    defer close(out)
    cur, _ := <- order
    count := 0
    for i := range f(ctx, reqIn, load, labels) {
      for ; i.Ref != cur.Ref; cur, _ = <- order {
        if count == 0 {
          out <- gdbi.ElementLookup{ID:cur.ID, Ref:cur.Ref}
        }
        count = 0
      }
      out <- i
      count++
    }
    for r := range order {
      out <- gdbi.ElementLookup{ID:r.ID, Ref:r.Ref}
    }
  }()
  return out
}
