package gdbi

import (
	"fmt"
	"sync"
)

type MemQueue struct {
	input  chan Traveler
	output chan Traveler
}

type Queue interface {
	GetInput() chan Traveler
	GetOutput() chan Traveler
}

func NewQueue() Queue {
	o := MemQueue{
		input:  make(chan Traveler, 50),
		output: make(chan Traveler, 50),
	}
	queue := make([]Traveler, 0, 1000)
	closed := false
	m := &sync.Mutex{}
	inCount := 0
	outCount := 0
	go func() {
		for i := range o.input {
			m.Lock()
			inCount++
			if i.IsSignal() {
				//fmt.Printf("Queue got signal %d\n", i.Signal.ID)
			}
			//fmt.Printf("Queue Size: %d %d / %d\n", len(queue), inCount, outCount)
			queue = append(queue, i)
			m.Unlock()
		}
		closed = true
	}()
	go func() {
		defer close(o.output)
		for running := true; running; {
			var v Traveler
			m.Lock()
			if len(queue) > 0 {
				v = queue[0]
				queue = queue[1:]
			} else {
				if closed {
					running = false
				}
			}
			m.Unlock()
			if v != nil {
				o.output <- v
				outCount++
			}
		}
		fmt.Printf("Closing Queue Size: %d %d / %d\n", len(queue), inCount, outCount)
		fmt.Printf("Closing Buffered Queue\n")
	}()
	return &o
}

func (q *MemQueue) GetInput() chan Traveler {
	return q.input
}

func (q *MemQueue) GetOutput() chan Traveler {
	return q.output
}
