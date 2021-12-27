package gdbi

import (
  "context"
)

type RetrievedData struct {
	Req  ElementLookup
	Data interface{}
}


type LoadData func(req ElementLookup, load bool) chan interface{}

type Deserialize func(req ElementLookup, data interface{}) ElementLookup

func DualProcessor(ctx context.Context, reqChan chan ElementLookup, load bool, loader LoadData, deserializer Deserialize) chan ElementLookup {

  data := make(chan RetrievedData, 100)

	go func() {
		defer close(data)
    for r := range reqChan {
      if r.Ref != nil && r.Ref.Signal != nil {
        data <- RetrievedData{Req: r}
      } else {
        for out := range loader(r, load) {
    				data <- RetrievedData{
    					Req:  r,
    					Data: out,
    				}
    		}
      }
    }
	}()

	out := make(chan ElementLookup, 100)
	go func() {
		defer close(out)
		for d := range data {
      if d.Req.Ref != nil && d.Req.Ref.Signal != nil {
        out <- d.Req
      } else {
        o := deserializer( d.Req, d.Data )
        out <- o
      }
		}
	}()

  return out
}
