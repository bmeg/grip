package jobstorage

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type Stream struct {
	Pipe      gdbi.InPipe
	DataType  gdbi.DataType
	MarkTypes map[string]gdbi.DataType
	Query     []*gripql.GraphStatement
}

type JobStorage interface {
	List(graph string) (chan string, error)
	Search(graph string, Query []*gripql.GraphStatement) (chan *gripql.JobStatus, error)
	Spool(graph string, stream *Stream) (string, error)
	Stream(ctx context.Context, graph, id string) (*Stream, error)
	Delete(graph, id string) error
	Status(graph, id string) (*gripql.JobStatus, error)
}

type Job struct {
	Status        gripql.JobStatus
	DataType      gdbi.DataType
	MarkTypes     map[string]gdbi.DataType
	StepChecksums []string
}
