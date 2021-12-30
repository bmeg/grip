package logic

import (
	"context"
	"fmt"
	"github.com/bmeg/grip/engine/queue"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"time"
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

		mCount := 0
		for inputOpen := true; inputOpen; {
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
						fmt.Printf("Got input close, messages: %d\n", mCount)
						inputOpen = false
					} else {
						out <- msg
						mCount++
					}
				default:
				}
			}
		}

		// during closing phase, the main input chase has been closed upstream,
		// but the jump inputs could still produce new inputs from travelers moving
		// in a cycle. When we observe there are now jump travelers received, we will
		// generate a signal and see if it some back before any other messages are received
		curID := 0       //id of signal that was sent
		returnCount := 0 //number of jumps that have returned the current signal
		//are we waiting for a signal. This is canceled if new travelers are received.
		signalOutdated := false
		signalActive := false
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
					} else {
						//jump traveler recieved, pass on and skip reading input this cycle
						if msg.Signal != nil {
							returnCount++
						} else {
							if signalActive {
								//fmt.Printf("Jumper found %s, signal %d outdated (%d)\n", msg.Current.ID, curID, mCount)
								signalOutdated = true
							}
							jumperFound = true
							out <- msg
							mCount++
						}
					}
				default:
					//if jump input produce no messages, leave jumperFound false
					time.Sleep(time.Microsecond)
				}
			}
			for _, i := range closeList {
				s.inputs = append(s.inputs[:i], s.inputs[i+1:]...)
			}

			if !jumperFound {
				if (!signalActive && !signalOutdated) || (signalOutdated && returnCount == len(s.inputs)) {
					curID++
					signalActive = true
					signalOutdated = false
					returnCount = 0
					fmt.Printf("Sending Signal %d\n", curID)
					out <- &gdbi.Traveler{Signal: &gdbi.Signal{ID: curID, Dest: s.Name}}
				} else if signalActive && returnCount == len(s.inputs) {
					fmt.Printf("Received %d of %d signals, closing after %d messages\n", returnCount, len(s.inputs), mCount)
					closed = true
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
	jumpers chan *gdbi.Traveler
	queue   queue.Queue
}

func (s *Jump) Init() {
	q := queue.New()
	s.jumpers = q.GetInput()
	s.queue = q
}

func (s *Jump) GetJumpOutput() chan *gdbi.Traveler {
	return s.queue.GetOutput()
}

func (s *Jump) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		defer close(s.jumpers)
		mCount := 0
		canceled := false
		for t := range in {
			select {
			case <-ctx.Done():
				//once canceled, we will continue to pass read the input, and pass to
				//the output, but we will stop sending jumpers, so that infinite loops
				//don't happen
				canceled = true
			default:
			}
			if t.Signal != nil {
				// If receiving a signal from the destintion marker, send it forward
				if t.Signal.Dest == s.Mark {
					s.jumpers <- t
				}
				out <- t
				continue
			}
			if s.Stmt == nil || MatchesHasExpression(t, s.Stmt) {
				if !canceled {
					s.jumpers <- t
				}
			}
			if s.Emit {
				out <- t.Copy()
				mCount++
			}
		}
		fmt.Printf("Closing jump, messages: %d\n", mCount)
	}()
	return ctx
}
