package pipeline

import (
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
)

type StepLogger struct {
	Counts  uint64
	in, out chan gdbi.Traveler
}

type PipelineLogger struct {
	steps map[string]*StepLogger
}

func NewPipelineLogger() *PipelineLogger {
	return &PipelineLogger{steps: map[string]*StepLogger{}}
}

func (pl *PipelineLogger) AddStep(name string, in chan gdbi.Traveler) chan gdbi.Traveler {
	s := &StepLogger{
		Counts: 0,
		in:     in,
		out:    make(chan gdbi.Traveler, 10),
	}
	go func(a *StepLogger) {
		defer close(a.out)
		for i := range a.in {
			a.Counts++
			a.out <- i
		}
	}(s)
	pl.steps[name] = s
	return s.out
}

func (pl *PipelineLogger) Log() {
	for n, c := range pl.steps {
		log.Debugf("step count: %s %d", n, c.Counts)
	}

}
