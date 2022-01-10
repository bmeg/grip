package gdbi

import (
	"context"
	"fmt"
	"time"
)

type RetrievedData struct {
	Req  ElementLookup
	Data interface{}
}

func (tr *BaseTraveler) GetSignal() Signal {
	if tr.Signal == nil {
		return Signal{}
	}
	return *tr.Signal
}

func (tr *BaseTraveler) IsSignal() bool {
	if tr.Signal != nil {
		return true
	}
	return false
}

func (req ElementLookup) IsSignal() bool {
	if req.Ref != nil && req.Ref.IsSignal() {
		return true
	}
	return false
}

type LoadData func(req ElementLookup, load bool) chan interface{}

type Deserialize func(req ElementLookup, data interface{}) ElementLookup

func DualProcessor(ctx context.Context, reqChan chan ElementLookup, load bool, loader LoadData, deserializer Deserialize) chan ElementLookup {

	data := make(chan RetrievedData, 100)

	go func() {
		defer close(data)
		for r := range reqChan {
			if r.IsSignal() {
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
			if d.Req.IsSignal() {
				out <- d.Req
			} else {
				o := deserializer(d.Req, d.Data)
				out <- o
			}
		}
	}()

	return out
}

func LookupBatcher(req chan ElementLookup, batchSize int, timeout time.Duration) chan []ElementLookup {
	out := make(chan []ElementLookup, 100)

	go func() {
		defer close(out)
		o := make([]ElementLookup, 0, batchSize)
		last := time.Now()
		for open := true; open; {
			select {
			case e, ok := <-req:
				last = time.Now()
				if ok {
					o = append(o, e)
				} else {
					fmt.Printf("Batcher input closed\n")
					open = false
				}
			default:
				time.Sleep(timeout / 4)
			}
			if len(o) > 0 {
				if len(o) >= batchSize || time.Since(last) > timeout {
					out <- o
					o = make([]ElementLookup, 0, batchSize)
					last = time.Now()
				}
			}
		}
		if len(o) > 0 {
			out <- o
		}
	}()

	return out
}
