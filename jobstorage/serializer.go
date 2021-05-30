package jobstorage

import (
	"encoding/json"

	"github.com/bmeg/grip/gdbi"
)

func MarshalStream(inPipe gdbi.InPipe, nworkers int) chan []byte {

	toWorkers := make([]chan *gdbi.Traveler, nworkers)
	fromWorkers := make([]chan []byte, nworkers)
	for i := 0; i < nworkers; i++ {
		toWorkers[i] = make(chan *gdbi.Traveler, 10)
		fromWorkers[i] = make(chan []byte, 10)
	}

	//workers to do the serialization
	for i := 0; i < nworkers; i++ {
		go func(in chan *gdbi.Traveler, out chan []byte) {
			defer close(out)
			for t := range in {
				b, _ := json.Marshal(t)
				out <- b
			}
		}(toWorkers[i], fromWorkers[i])
	}

	//read the inputs
	go func() {
		n := 0
		for i := range inPipe {
			toWorkers[n] <- i
			n++
			if n >= nworkers {
				n = 0
			}
		}
		for i := 0; i < nworkers; i++ {
			close(toWorkers[i])
		}
	}()

	out := make(chan []byte, nworkers*10)
	//merge the outputs
	go func() {
		defer close(out)
		for found := true; found; {
			found = false
			for i := 0; i < nworkers; i++ {
				if c, ok := <-fromWorkers[i]; ok {
					out <- c
					found = true
				}
			}
		}
	}()

	return out
}

func UnmarshalStream(inPipe chan []byte, nworkers int) chan *gdbi.Traveler {
	fromWorkers := make([]chan *gdbi.Traveler, nworkers)
	toWorkers := make([]chan []byte, nworkers)
	for i := 0; i < nworkers; i++ {
		fromWorkers[i] = make(chan *gdbi.Traveler, 10)
		toWorkers[i] = make(chan []byte, 10)
	}

	//workers to do the serialization
	for i := 0; i < nworkers; i++ {
		go func(in chan []byte, out chan *gdbi.Traveler) {
			defer close(out)
			for t := range in {
				b := &gdbi.Traveler{}
				json.Unmarshal(t, b)
				out <- b
			}
		}(toWorkers[i], fromWorkers[i])
	}

	//read the inputs
	go func() {
		n := 0
		for i := range inPipe {
			toWorkers[n] <- i
			n++
			if n >= nworkers {
				n = 0
			}
		}
		for i := 0; i < nworkers; i++ {
			close(toWorkers[i])
		}
	}()

	out := make(chan *gdbi.Traveler, nworkers*10)
	//merge the outputs
	go func() {
		defer close(out)
		for found := true; found; {
			found = false
			for i := 0; i < nworkers; i++ {
				if c, ok := <-fromWorkers[i]; ok {
					out <- c
					found = true
				}
			}
		}
	}()

	return out
}
