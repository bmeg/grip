package engine

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	//"log"
)

type Pipeline struct {
	procs    []gdbi.Processor
	dataType gdbi.DataType
}

func (pipe Pipeline) Start(bufsize int) gdbi.InPipe {
	if len(pipe.procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	in := make(chan *gdbi.Traveler)
	final := make(chan *gdbi.Traveler, bufsize)

	// Write an empty traveler to input
	// to trigger the computation.
	go initPipe(in)

	for i := 0; i < len(pipe.procs)-1; i++ {
		glue := make(chan *gdbi.Traveler, bufsize)
		go startOne(pipe.procs[i], in, glue)
		in = glue
	}

	last := pipe.procs[len(pipe.procs)-1]
	go startOne(last, in, final)

	return final
}

func (pipe Pipeline) Run() <-chan *aql.ResultRow {

	bufsize := 100
	resch := make(chan *aql.ResultRow, bufsize)

	go func() {
		defer close(resch)

		for t := range pipe.Start(bufsize) {
			resch <- t.Convert(pipe.dataType)
		}
	}()

	return resch
}

// Sends an empty traveler to the pipe to kick off pipelines of processors.
func initPipe(out gdbi.OutPipe) {
	out <- &gdbi.Traveler{}
	close(out)
}

func startOne(proc gdbi.Processor, in gdbi.InPipe, out gdbi.OutPipe) {
	proc.Process(in, out)
	close(out)
}
