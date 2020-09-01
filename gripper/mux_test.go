package gripper

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

var test_count = 100

func incPipeline() (chan interface{}, chan interface{}) {
	in := make(chan interface{})
	out := make(chan interface{})
	go func() {
		defer close(out)
		for i := range in {
			c := i.(int)
			n := rand.Intn(10)
			time.Sleep(time.Duration(n) * time.Millisecond)
			out <- c + 1
		}
	}()
	return in, out
}

func TestMux(t *testing.T) {
	mux := NewChannelMux()
	out := mux.GetOutChannel()

	wg := &sync.WaitGroup{}

	wg.Add(1)
	counted := 0
	go func() {
		for i := 0; i < test_count; i++ {
			o := <-out
			if o != i+1 {
				t.Errorf("Wrong order: %d != %d", o, i+1)
			}
			counted += 1
		}
		wg.Done()
	}()

	in1, out1 := incPipeline()
	in2, out2 := incPipeline()
	in3, out3 := incPipeline()

	m1, err := mux.AddPipeline(in1, out1)
	m2, err := mux.AddPipeline(in2, out2)
	m3, err := mux.AddPipeline(in3, out3)

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < test_count; i++ {
		switch rand.Intn(3) {
		case 0:
			mux.Put(m1, i)
		case 1:
			mux.Put(m2, i)
		case 2:
			mux.Put(m3, i)
		}
	}

	mux.Close()
	wg.Wait()
	if counted != test_count {
		t.Errorf("Incorrect output count: %d != %d", counted, test_count)
	}
}
