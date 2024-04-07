package gdbi

import (
	"sync"

	"github.com/bmeg/grip/log"
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
				log.Debugf("Queue got signal %d\n", i.GetSignal().ID)
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
		log.Debugf("Closing Queue Size: %d %d / %d\n", len(queue), inCount, outCount)
		log.Debugf("Closing Buffered Queue\n")
	}()
	return &o
}

func (q *MemQueue) GetInput() chan Traveler {
	return q.input
}

func (q *MemQueue) GetOutput() chan Traveler {
	return q.output
}
