/*
The engine package pulls togeather pipelines and runs processing
*/

package engine

import (
	"context"
	//"fmt"
	"log"

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
func Run(ctx context.Context, pipe gdbi.Pipeline, workdir string) <-chan *aql.QueryResult {
	bufsize := 5000
	resch := make(chan *aql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		for t := range Start(ctx, pipe, workdir, bufsize) {
			resch <- Convert(dataType, markTypes, t)
		}
	}()

	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(dataType gdbi.DataType, markTypes map[string]gdbi.DataType, t *gdbi.Traveler) *aql.QueryResult {
	switch dataType {
	case gdbi.VertexData:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Vertex{
				Vertex: t.GetCurrent().ToVertex(),
			},
		}

	case gdbi.EdgeData:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Edge{
				Edge: t.GetCurrent().ToEdge(),
			},
		}

	case gdbi.CountData:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Count{
				Count: t.Count,
			},
		}

	case gdbi.SelectionData:
		selections := map[string]*aql.Selection{}
		for k, v := range t.Selections {
			switch markTypes[k] {
			case gdbi.VertexData:
				selections[k] = &aql.Selection{
					&aql.Selection_Vertex{
						Vertex: v.ToVertex(),
					},
				}
			case gdbi.EdgeData:
				selections[k] = &aql.Selection{
					&aql.Selection_Edge{
						Edge: v.ToEdge(),
					},
				}
			}
		}
		return &aql.QueryResult{
			Result: &aql.QueryResult_Selections{
				Selections: &aql.Selections{
					Selections: selections,
				},
			},
		}

	case gdbi.RenderData:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Render{
				Render: protoutil.WrapValue(t.Render),
			},
		}

	case gdbi.AggregationData:
		return &aql.QueryResult{
			Result: &aql.QueryResult_Aggregations{
				Aggregations: &aql.NamedAggregationResult{
					Aggregations: t.Aggregations,
				},
			},
		}

	default:
		log.Printf("unhandled data type %d", dataType)
	}
	return nil
}
