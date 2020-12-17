/*
The pipeline connects togeather the processors
*/

package pipeline

import (
	"context"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
)

// Start begins processing a query pipeline
func Start(ctx context.Context, pipe gdbi.Pipeline, man gdbi.Manager, bufsize int) gdbi.InPipe {
	procs := pipe.Processors()
	if len(procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	in := make(chan *gdbi.Traveler, bufsize)
	final := make(chan *gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
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
func Run(ctx context.Context, pipe gdbi.Pipeline, workdir string) <-chan *gripql.QueryResult {
	bufsize := 5000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager(workdir)
		for t := range Start(ctx, pipe, man, bufsize) {
			resch <- Convert(dataType, markTypes, t)
		}
		man.Cleanup()
	}()

	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(dataType gdbi.DataType, markTypes map[string]gdbi.DataType, t *gdbi.Traveler) *gripql.QueryResult {
	switch dataType {
	case gdbi.VertexData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Vertex{
				Vertex: t.GetCurrent().ToVertex(),
			},
		}

	case gdbi.EdgeData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Edge{
				Edge: t.GetCurrent().ToEdge(),
			},
		}

	case gdbi.CountData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Count{
				Count: t.Count,
			},
		}

	case gdbi.SelectionData:
		selections := map[string]*gripql.Selection{}
		for k, v := range t.Selections {
			switch markTypes[k] {
			case gdbi.VertexData:
				selections[k] = &gripql.Selection{
					Result: &gripql.Selection_Vertex{
						Vertex: v.ToVertex(),
					},
				}
			case gdbi.EdgeData:
				selections[k] = &gripql.Selection{
					Result: &gripql.Selection_Edge{
						Edge: v.ToEdge(),
					},
				}
			}
		}
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Selections{
				Selections: &gripql.Selections{
					Selections: selections,
				},
			},
		}

	case gdbi.RenderData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Render{
				Render: protoutil.WrapValue(t.Render),
			},
		}

	case gdbi.AggregationData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Aggregations{
				Aggregations: &gripql.NamedAggregationResult{
					Name: t.Aggregation.Name,
					Key : protoutil.WrapValue(t.Aggregation.Key),
					Value: t.Aggregation.Value,
				},
			},
		}

	default:
		log.Errorf("unhandled data type %T", dataType)
	}
	return nil
}
