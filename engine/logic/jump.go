package logic

import (
	"context"
	"fmt"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// MarkJump creates mark where jump instruction can send travelers
type JumpMark struct {
	Name   string
	inputs []chan *gdbi.Traveler
}

// Process runs Selector
func (s *JumpMark) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		mOpen := true
		for {
			jumperFound := false
			if s.inputs != nil {
        jClosed := 0
				for i := range s.inputs {
					select {
					case msg, ok := <-s.inputs[i]:
						if !ok {
              jClosed++
              fmt.Printf("j closed %s %s %s\n", len(s.inputs), jClosed, mOpen)
            } else {
							fmt.Printf("Passing jumper\n")
							jumperFound = true
							out <- msg
						}
					default:
					}
				}
			}
			if !jumperFound {
				select {
				case msg, ok := <-in:
					if !ok {
						if mOpen {
							fmt.Printf("Start closing: %s\n", msg)
							mOpen = false
							out <- &gdbi.Traveler{Signal: true, SignalDest: s.Name}
						}
					} else {
						fmt.Printf("Passing input\n")
						out <- msg
					}
				default:
				}
			}
		}
	}()
	return ctx
}

func (s *JumpMark) AddInput(in chan *gdbi.Traveler) {
	if s.inputs == nil {
		s.inputs = []chan *gdbi.Traveler{in}
	} else {
		s.inputs = append(s.inputs, in)
	}
}

type Jump struct {
	Mark    string
	Stmt    *gripql.HasExpression
	Emit    bool
	Jumpers chan *gdbi.Traveler
}

func (s *Jump) Init() {
	s.Jumpers = make(chan *gdbi.Traveler, 10)
}

func (s *Jump) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		defer close(s.Jumpers)
    sigState := false
		for t := range in {
			if t.Signal {
        if t.SignalDest == s.Mark {
          fmt.Printf("Got mark close signal\n")
          if sigState {
            close(s.Jumpers)
          } else {
            s.Jumpers <- t
            sigState = true
          }
				}
				out <- t
				continue
			}
			fmt.Printf("Jump got input\n")
      sigState = false
			if s.Stmt == nil || MatchesHasExpression(t, s.Stmt) {
				s.Jumpers <- t
			}
			if s.Emit {
				out <- t.Copy()
			}
		}
		fmt.Printf("Closing jump\n")
	}()
	return ctx
}
