package engine

import (
	//"log"
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
)

// Pipeline a set of runnable query operations
type Pipeline struct {
	procs     []Processor
	dataType  gdbi.DataType
	markTypes map[string]gdbi.DataType
	rowTypes  []gdbi.DataType
	workDir   string
}

type propKey string

var propLoad propKey = "load"

func getPropLoad(ctx context.Context) bool {
	return ctx.Value(propLoad).(bool)
}

// Start begins processing a query pipeline
func (pipe Pipeline) Start(ctx context.Context, bufsize int) gdbi.InPipe {
	if len(pipe.procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	ctx = context.WithValue(ctx, propLoad, true)

	in := make(chan *gdbi.Traveler, bufsize)
	final := make(chan *gdbi.Traveler, bufsize)
	out := final
	for i := len(pipe.procs) - 1; i >= 0; i-- {
		man := pipe.NewManager()
		ctx = pipe.procs[i].Process(ctx, man, in, out)
		out = in
		in = make(chan *gdbi.Traveler, bufsize)
	}

	// Write an empty traveler to input
	// to trigger the computation.
	// Sends an empty traveler to the pipe to kick off pipelines of processors.
	go func() {
		out <- &gdbi.Traveler{}
		close(in)
		close(out)
	}()
	return final
}

// Run starts a pipeline and converts the output to server output structures
func (pipe Pipeline) Run(ctx context.Context) <-chan *aql.ResultRow {

	bufsize := 100
	resch := make(chan *aql.ResultRow, bufsize)

	go func() {
		defer close(resch)
		for t := range pipe.Start(ctx, bufsize) {
			resch <- pipe.Convert(t)
		}
	}()

	return resch
}

// Convert takes a traveler and converts it to query output
func (pipe Pipeline) Convert(t *gdbi.Traveler) *aql.ResultRow {
	switch pipe.dataType {
	case gdbi.VertexData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Vertex{
					Vertex: t.GetCurrent().ToVertex(),
				},
			},
		}

	case gdbi.EdgeData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Edge{
					Edge: t.GetCurrent().ToEdge(),
				},
			},
		}

	case gdbi.CountData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Data{
					Data: protoutil.WrapValue(t.Count),
				},
			},
		}

	case gdbi.GroupCountData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Data{
					Data: protoutil.WrapValue(t.GroupCounts),
				},
			},
		}

	case gdbi.RowData:
		res := &aql.ResultRow{}
		for i, r := range t.GetCurrent().Row {
			if pipe.rowTypes[i] == gdbi.VertexData {
				elem := &aql.QueryResult{
					Result: &aql.QueryResult_Vertex{
						Vertex: r.ToVertex(),
					},
				}
				res.Row = append(res.Row, elem)
			} else if pipe.rowTypes[i] == gdbi.EdgeData {
				elem := &aql.QueryResult{
					Result: &aql.QueryResult_Edge{
						Edge: r.ToEdge(),
					},
				}
				res.Row = append(res.Row, elem)
			}
		}
		return res

	case gdbi.ValueData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				Result: &aql.QueryResult_Data{
					Data: protoutil.WrapValue(t.GetCurrent().Data),
				},
			},
		}

	default:
		panic(fmt.Errorf("unhandled data type %d", pipe.dataType))
	}
}
