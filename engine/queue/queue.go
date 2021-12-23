package queue

import (
  "fmt"
  "sync"
  "github.com/bmeg/grip/gdbi"
)

type MemQueue struct {
  input chan *gdbi.Traveler
  output chan *gdbi.Traveler
}

type Queue interface {
  GetInput() chan *gdbi.Traveler
  GetOutput() chan *gdbi.Traveler
}


func New() Queue {
  o := MemQueue{
    input: make(chan *gdbi.Traveler),
    output: make(chan *gdbi.Traveler),
  }
  queue := make([]*gdbi.Traveler, 0, 1000)
  closed := false
  m := &sync.Mutex{}
  inCount := 0
  outCount := 0
  go func() {
    for i := range o.input {
      m.Lock()
      inCount++
      if i.Signal != nil {
        fmt.Printf("Queue got signal\n")
      }
      fmt.Printf("Queue Size: %d %d / %d\n", len(queue), inCount, outCount)
      queue = append(queue, i)
      m.Unlock()
    }
    closed = true
  }()
  go func() {
    defer close(o.output)
    for running := true; running ;{
      var v *gdbi.Traveler
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

func (q *MemQueue) GetInput() chan *gdbi.Traveler {
  return q.input
}

func (q *MemQueue) GetOutput() chan *gdbi.Traveler {
  return q.output
}
