package engine

import (
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvi"
)

type Manager interface {
	//Get handle to temporary KeyValue store driver
	GetTempKV() kvi.KVInterface

	Cleanup()
}

// Processor is the interface for a step in the pipe engine
type Processor interface {
	//DataType() DataType
	Process(man Manager, in gdbi.InPipe, out gdbi.OutPipe)
}
