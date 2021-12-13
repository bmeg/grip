/*
The pipeline connects togeather the processors
*/

package pipeline

import (
	"context"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/types/known/structpb"
)

// Start begins processing a query pipeline
func Start(ctx context.Context, pipe gdbi.Pipeline, man gdbi.Manager, bufsize int, input gdbi.InPipe, cancel func()) gdbi.InPipe {
	procs := pipe.Processors()
	if len(procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	markProcs := map[string]*logic.JumpMark{}
	for i := range procs {
		if p, ok := procs[i].(*logic.JumpMark); ok {
			markProcs[p.Name] = p
		}
	}
	for i := range procs {
		if p, ok := procs[i].(*logic.Jump); ok {
			if d, ok := markProcs[p.Mark]; ok {
				p.Init()
				d.AddInput(p.Jumpers)
			} else {
				log.Errorf("Missing Jump Mark")
			}
		}
	}

	in := make(chan *gdbi.Traveler, bufsize)
	final := make(chan *gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
		ctx = procs[i].Process(ctx, man, in, out)
		out = in
		in = make(chan *gdbi.Traveler, bufsize)
	}

	go func() {
		if input != nil {
			for i := range input {
				if ctx.Err() == context.Canceled {
					//cancel upstream
					cancel()
				}
				out <- i
			}
		} else {
			// Write an empty traveler to input
			// to trigger the computation.
			// Sends an empty traveler to the pipe to kick off pipelines of processors.
			out <- &gdbi.Traveler{}
		}
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
		graph := pipe.Graph()
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager(workdir)
		for t := range Start(ctx, pipe, man, bufsize, nil, nil) {
			resch <- Convert(graph, dataType, markTypes, t)
		}
		man.Cleanup()
	}()
	return resch
}

// Run starts a pipeline and converts the output to server output structures
func Resume(ctx context.Context, pipe gdbi.Pipeline, workdir string, input gdbi.InPipe, cancel func()) <-chan *gripql.QueryResult {
	bufsize := 5000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		graph := pipe.Graph()
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager(workdir)
		for t := range Start(ctx, pipe, man, bufsize, input, cancel) {
			resch <- Convert(graph, dataType, markTypes, t)
		}
		man.Cleanup()
	}()
	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(graph gdbi.GraphInterface, dataType gdbi.DataType, markTypes map[string]gdbi.DataType, t *gdbi.Traveler) *gripql.QueryResult {
	switch dataType {
	case gdbi.VertexData:
		ve := t.GetCurrent()
		if !ve.Loaded {
			//log.Infof("Loading output vertex: %s", ve.ID)
			//TODO: doing single vertex queries is slow.
			// Need to rework this to do batched queries
			ve = graph.GetVertex(ve.ID, true)
		}
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Vertex{
				Vertex: ve.ToVertex(),
			},
		}

	case gdbi.EdgeData:
		ee := t.GetCurrent()
		if !ee.Loaded {
			ee = graph.GetEdge(ee.ID, true)
		}
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Edge{
				Edge: ee.ToEdge(),
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
		sValue, _ := structpb.NewValue(t.Render)
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Render{
				Render: sValue,
			},
		}

	case gdbi.PathData:
		o := make([]interface{}, len(t.Path))
		for i := range t.Path {
			j := map[string]interface{}{}
			if t.Path[i].Vertex != "" {
				j["vertex"] = t.Path[i].Vertex
			} else if t.Path[i].Edge != "" {
				j["edge"] = t.Path[i].Edge
			}
			o[i] = j
		}
		sValue, _ := structpb.NewList(o)
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Path{
				Path: sValue,
			},
		}

	case gdbi.AggregationData:
		sValue, _ := structpb.NewValue(t.Aggregation.Key)
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Aggregations{
				Aggregations: &gripql.NamedAggregationResult{
					Name:  t.Aggregation.Name,
					Key:   sValue,
					Value: t.Aggregation.Value,
				},
			},
		}

	default:
		log.Errorf("unhandled data type %T", dataType)
	}
	return nil
}
