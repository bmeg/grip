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

		for inputOpen := true; inputOpen ; {
			jumperFound := false
			if s.inputs != nil {
        closeList := []int{}
				for i := range s.inputs {
					select {
					case msg, ok := <-s.inputs[i]:
						if !ok {
              //jump point has closed, remove it from list
              fmt.Printf("j closed %s \n", i)
              closeList = append(closeList, i)
            } else {
              //jump traveler recieved, pass on and skip reading input this cycle
							jumperFound = true
							out <- msg
						}
					default:
            //if jump input produce no messages, leave jumperFound false
					}
				}
				// jumps that are ahead of a mark can close before the mark
				// gets the close
        for _, i := range closeList {
          s.inputs = append(s.inputs[:i], s.inputs[i+1:]...)
        }
			}
			if !jumperFound {
				select {
				case msg, ok := <-in:
					if !ok {
            //main input has closed, move onto closing phase
            inputOpen = false
					} else {
						out <- msg
					}
				default:
				}
			}
		}

    // during closing phase, the main input chase has been closed upstream,
    // but the jump inputs could still produce new inputs from travelers moving
    // in a cycle. When we observe there are now jump travelers received, we will
    // generate a signal and see if it some back before any other messages are received
    curID := 0  //id of signal that was sent
    returnCount := 0  //number of jumps that have returned the current signal
		//are we waiting for a signal. This is canceled if new travelers are received.
    signalOngoing := false
    fmt.Printf("Starting preclose\n")
    for closed := false; !closed; {
      closeList := []int{}
      jumperFound := false
      for i := range s.inputs {
        select {
        case msg, ok := <-s.inputs[i]:
          if !ok {
            //jump point has closed, remove it from list
            fmt.Printf("j closed %s \n", i)
            closeList = append(closeList, i)
						signalOngoing = false
          } else {
            //jump traveler recieved, pass on and skip reading input this cycle
            if msg.Signal != nil {
              if signalOngoing && msg.Signal.ID == curID {
                returnCount++
              }
            } else {
              signalOngoing = false
              jumperFound = true
              out <- msg
            }
          }
        default:
          //if jump input produce no messages, leave jumperFound false
        }
      }
      for _, i := range closeList {
        s.inputs = append(s.inputs[:i], s.inputs[i+1:]...)
      }

      if jumperFound {
        signalOngoing = false
      } else {
        if !signalOngoing {
					curID++
					signalOngoing = true
					returnCount = 0
					out <- &gdbi.Traveler{ Signal: &gdbi.Signal{ID:curID, Dest:s.Name} }
				} else {
					if returnCount == len(s.inputs) {
						fmt.Printf("Received %d of %d signals\n", returnCount, len(s.inputs))
						closed = true
					}
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
		for t := range in {
			if t.Signal != nil {
        // If receiving a signal from the destintion marker, send it forward
        if t.Signal.Dest == s.Mark {
          s.Jumpers <- t
				}
				out <- t
				continue
			}
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
