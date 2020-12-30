package gripper

/*
Storage queue. Meant to be more inspectable then a channel, never block
writes and in future versions store data to disk if the queue gets too large
*/

import (
	"time"

	"github.com/Workiva/go-datastructures/queue"
)

// TODO: explore disk queue: https://github.com/joncrlsn/dque

func runQueue(in, out chan interface{}) {
	q := queue.New(1024)
	done := false
	go func() {
		for i := range in {
			q.Put(i)
		}
		done = true
	}()

	go func() {
		for {
			i, err := q.Poll(1, time.Millisecond)
			if err == nil {
				out <- i
			} else {
				if done {
					break
				}
			}
		}
		close(out)
	}()
}

func NewQueue(path string) (chan<- interface{}, <-chan interface{}) {
	in := make(chan interface{}, 10)
	out := make(chan interface{}, 10)
	go runQueue(in, out)
	return in, out
}
