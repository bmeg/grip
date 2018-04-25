/*
The engine package pulls togeather pipelines and runs processing
*/

package engine

import (
	"context"
	"fmt"
	//"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
)

// Start begins processing a query pipeline
func Start(ctx context.Context, pipe gdbi.Pipeline, workdir string, bufsize int) gdbi.InPipe {
	procs := pipe.Processors()
	if len(procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	//ctx = context.WithValue(ctx, propLoad, true)

	in := make(chan *gdbi.Traveler, bufsize)
	final := make(chan *gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
		man := NewManager(workdir)
		ctx = procs[i].Process(ctx, man, in, out)
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
func Run(ctx context.Context, pipe gdbi.Pipeline, workdir string) <-chan *aql.ResultRow {
	bufsize := 5000
	resch := make(chan *aql.ResultRow, bufsize)
	go func() {
		defer close(resch)
		dataType := pipe.DataType()
		rowTypes := pipe.RowTypes()
		for t := range Start(ctx, pipe, workdir, bufsize) {
			resch <- Convert(dataType, rowTypes, t)
		}
	}()

	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(dataType gdbi.DataType, rowTypes []gdbi.DataType, t *gdbi.Traveler) *aql.ResultRow {
	switch dataType {
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
			if rowTypes[i] == gdbi.VertexData {
				elem := &aql.QueryResult{
					Result: &aql.QueryResult_Vertex{
						Vertex: r.ToVertex(),
					},
				}
				res.Row = append(res.Row, elem)
			} else if rowTypes[i] == gdbi.EdgeData {
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
					Data: protoutil.WrapValue(t.GetCurrent().Value),
				},
			},
		}

	default:
		panic(fmt.Errorf("unhandled data type %d", dataType))
	}
}
