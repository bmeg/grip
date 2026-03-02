/*
The pipeline connects togeather the processors
*/

package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/types/known/structpb"
)

var debug = true

type RunningPipeline struct {
	Outputs gdbi.InPipe
	Logger  *PipelineLogger
}

func rowNeedsLoad(r gdbi.Row) bool {
	if r == nil {
		return false
	}
	switch r.Mode() {
	case gdbi.RowModeReference:
		return true
	case gdbi.RowModeRaw:
		return r.GetRaw() == ""
	case gdbi.RowModeProjected, gdbi.RowModeMaterialized:
		return false
	default:
		return !r.IsLoaded() && r.GetRaw() == ""
	}
}

func requiresPathTracking(pipe gdbi.Pipeline, procs []gdbi.Processor) bool {
	if pipe != nil && pipe.DataType() == gdbi.PathData {
		return true
	}
	for i := range procs {
		if p, ok := procs[i].(gdbi.PathTrackingProcessor); ok && p.RequiresPathTracking() {
			return true
		}
	}
	return false
}

// Start begins processing a query pipeline
func Start(ctx context.Context, pipe gdbi.Pipeline, man gdbi.Manager, bufsize int, input gdbi.InPipe, cancel func()) *RunningPipeline {
	procs := pipe.Processors()
	if len(procs) == 0 {
		log.Debugf("User query has no steps")
		ch := make(chan gdbi.Traveler)
		close(ch)
		return nil
	}

	markProcs := map[string]*logic.JumpMark{}
	for i := range procs {
		if p, ok := procs[i].(*logic.JumpMark); ok {
			markProcs[p.Name] = p
		}
	}

	// if there is a jump statement, connect to back to the mark statement
	for i := range procs {
		if p, ok := procs[i].(*logic.Jump); ok {
			if d, ok := markProcs[p.Mark]; ok {
				p.Init()
				d.AddInput(p.GetJumpOutput())
			} else {
				log.Debugf("User query missing Jump Mark")
				ch := make(chan gdbi.Traveler)
				close(ch)
				return nil
			}
		}
	}

	l := NewPipelineLogger()
	trackPath := requiresPathTracking(pipe, procs)

	in := make(chan gdbi.Traveler, bufsize)
	final := make(chan gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
		if debug {
			in = l.AddStep(fmt.Sprintf("%T_%d", procs[i], i), in)
		}
		ctx = procs[i].Process(ctx, man, in, out)
		out = in
		in = make(chan gdbi.Traveler, bufsize)
	}

	go func() {
		if input != nil {
			inputCount := uint64(0)
			for i := range input {
				if ctx.Err() == context.Canceled {
					//cancel upstream
					cancel()
				}
				if trackPath {
					if bt, ok := i.(*gdbi.BaseTraveler); ok && !bt.TrackPath && bt.Path == nil {
						cpy := *bt
						cpy.TrackPath = true
						i = &cpy
					}
				}
				inputCount++
				out <- i
			}
			log.Debugf("Stream input count: %d", inputCount)
		} else {
			// Write an empty traveler to input
			// to trigger the computation.
			// Sends an empty traveler to the pipe to kick off pipelines of processors.
			out <- &gdbi.BaseTraveler{TrackPath: trackPath}
		}
		close(in)
		close(out)
	}()
	return &RunningPipeline{
		Outputs: final,
		Logger:  l,
	}
}

// Run starts a pipeline and converts the output to server output structures
func Run(ctx context.Context, pipe gdbi.Pipeline, workdir string) <-chan *gripql.QueryResult {
	bufsize := 20000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		graph := pipe.Graph()
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager(workdir)
		rPipe := Start(ctx, pipe, man, bufsize, nil, nil)
		var batch []gdbi.Traveler
		var totalConverted int
		pipelineStart := time.Now()
		for t := range rPipe.Outputs {
			if !t.IsSignal() {
				batch = append(batch, t)
				if len(batch) >= bufsize {
					convertStart := time.Now()
					converted := BatchConvert(ctx, graph, dataType, markTypes, batch)
					convertElapsed := time.Since(convertStart)
					var emitted int
					for _, c := range converted {
						if c != nil {
							resch <- c
							emitted++
						}
					}
					totalConverted += emitted
					log.Debugf("pipeline.Run batch dataType=%s in=%d out=%d convert=%s", dataType, len(batch), emitted, convertElapsed.Round(time.Millisecond))
					batch = nil
				}
			}
		}
		if len(batch) > 0 {
			convertStart := time.Now()
			converted := BatchConvert(ctx, graph, dataType, markTypes, batch)
			convertElapsed := time.Since(convertStart)
			var emitted int
			for _, c := range converted {
				if c != nil {
					resch <- c
					emitted++
				}
			}
			totalConverted += emitted
			log.Debugf("pipeline.Run tail dataType=%s in=%d out=%d convert=%s", dataType, len(batch), emitted, convertElapsed.Round(time.Millisecond))
		}
		log.Debugf("pipeline.Run complete dataType=%s out=%d elapsed=%s", dataType, totalConverted, time.Since(pipelineStart).Round(time.Millisecond))
		man.Cleanup()
	}()
	return resch
}

// Run starts a pipeline and converts the output to server output structures
func Resume(ctx context.Context, pipe gdbi.Pipeline, workdir string, input gdbi.InPipe, cancel func()) <-chan *gripql.QueryResult {
	bufsize := 20000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		graph := pipe.Graph()
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		man := engine.NewManager(workdir)
		log.Debugf("resuming: out %s", dataType)
		rPipe := Start(ctx, pipe, man, bufsize, input, cancel)
		if rPipe != nil {
			var batch []gdbi.Traveler
			var totalConverted int
			pipelineStart := time.Now()
			for t := range rPipe.Outputs {
				if !t.IsSignal() {
					batch = append(batch, t)
					if len(batch) >= bufsize {
						convertStart := time.Now()
						converted := BatchConvert(ctx, graph, dataType, markTypes, batch)
						convertElapsed := time.Since(convertStart)
						var emitted int
						for _, c := range converted {
							if c != nil {
								resch <- c
								emitted++
							}
						}
						totalConverted += emitted
						log.Debugf("pipeline.Resume batch dataType=%s in=%d out=%d convert=%s", dataType, len(batch), emitted, convertElapsed.Round(time.Millisecond))
						batch = nil
					}
				}
			}
			if len(batch) > 0 {
				convertStart := time.Now()
				converted := BatchConvert(ctx, graph, dataType, markTypes, batch)
				convertElapsed := time.Since(convertStart)
				var emitted int
				for _, c := range converted {
					if c != nil {
						resch <- c
						emitted++
					}
				}
				totalConverted += emitted
				log.Debugf("pipeline.Resume tail dataType=%s in=%d out=%d convert=%s", dataType, len(batch), emitted, convertElapsed.Round(time.Millisecond))
			}
			log.Debugf("pipeline.Resume complete dataType=%s out=%d elapsed=%s", dataType, totalConverted, time.Since(pipelineStart).Round(time.Millisecond))
			if debug {
				rPipe.Logger.Log()
			}
		}
		man.Cleanup()
	}()
	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(graph gdbi.GraphInterface, dataType gdbi.DataType, markTypes map[string]gdbi.DataType, t gdbi.Traveler) *gripql.QueryResult {
	switch dataType {
	case gdbi.VertexData:
		ve := t.GetCurrent()
		if ve != nil {
			if rowNeedsLoad(ve) {
				ve = graph.GetVertex(ve.GetID(), true)
			}
			if ve == nil {
				return nil
			}
			return &gripql.QueryResult{
				Result: &gripql.QueryResult_Vertex{
					Vertex: gdbi.RowToVertex(ve),
				},
			}
		} else {
			return &gripql.QueryResult{Result: &gripql.QueryResult_Vertex{}}
		}

	case gdbi.EdgeData:
		ee := t.GetCurrent()
		if ee != nil {
			if rowNeedsLoad(ee) {
				ee = graph.GetEdge(ee.GetID(), true)
			}
			if ee == nil {
				return nil
			}
			return &gripql.QueryResult{
				Result: &gripql.QueryResult_Edge{
					Edge: gdbi.RowToEdge(ee),
				},
			}
		} else {
			return &gripql.QueryResult{Result: &gripql.QueryResult_Edge{}}
		}

	case gdbi.CountData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Count{
				Count: t.GetCount(),
			},
		}

	case gdbi.RenderData:
		sValue, _ := structpb.NewValue(t.GetRender())
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Render{
				Render: sValue,
			},
		}

	case gdbi.PathData:
		path := t.GetPath()
		o := make([]any, len(path))

		for i := range path {
			j := map[string]any{}
			if path[i].Vertex != "" {
				j["vertex"] = path[i].Vertex
			} else if path[i].Edge != "" {
				j["edge"] = path[i].Edge
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
		agg := t.GetAggregation()
		sValue, _ := structpb.NewValue(agg.Key)
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Aggregations{
				Aggregations: &gripql.NamedAggregationResult{
					Name:  agg.Name,
					Key:   sValue,
					Value: agg.Value,
				},
			},
		}

	default:
		log.Errorf("unhandled data type %T", dataType)
	}
	return nil
}

func BatchConvert(ctx context.Context, graph gdbi.GraphInterface, dataType gdbi.DataType, markTypes map[string]gdbi.DataType, travelers []gdbi.Traveler) []*gripql.QueryResult {
	if len(travelers) == 0 {
		return nil
	}
	results := make([]*gripql.QueryResult, len(travelers))

	if dataType == gdbi.VertexData {
		type vertexResult struct {
			idx int
			ve  gdbi.Row
		}
		loadedVerts := make([]vertexResult, 0, len(travelers))
		reqChan := make(chan gdbi.ElementLookup, len(travelers))
		pending := 0
		for i, t := range travelers {
			ve := t.GetCurrent()
			if ve != nil {
				if rowNeedsLoad(ve) {
					reqChan <- gdbi.ElementLookup{ID: ve.GetID(), Ref: t}
					pending++
				} else {
					loadedVerts = append(loadedVerts, vertexResult{idx: i, ve: ve})
				}
			}
		}
		close(reqChan)

		if pending > 0 {
			tToIdx := make(map[gdbi.Traveler]int)
			for i, t := range travelers {
				tToIdx[t] = i
			}

			outChan := graph.GetVertexChannel(ctx, reqChan, true)
			for lookup := range outChan {
				idx := tToIdx[lookup.Ref]
				if lookup.Vertex != nil {
					loadedVerts = append(loadedVerts, vertexResult{idx: idx, ve: lookup.Vertex})
				}
			}
		}

		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		if workers > len(loadedVerts) {
			workers = len(loadedVerts)
		}
		if workers <= 1 {
			for _, item := range loadedVerts {
				if item.ve == nil {
					continue
				}
				results[item.idx] = &gripql.QueryResult{
					Result: &gripql.QueryResult_Vertex{
						Vertex: gdbi.RowToVertex(item.ve),
					},
				}
			}
		} else {
			jobs := make(chan vertexResult, workers*2)
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for item := range jobs {
						if item.ve == nil {
							continue
						}
						results[item.idx] = &gripql.QueryResult{
							Result: &gripql.QueryResult_Vertex{
								Vertex: gdbi.RowToVertex(item.ve),
							},
						}
					}
				}()
			}
			for _, item := range loadedVerts {
				jobs <- item
			}
			close(jobs)
			wg.Wait()
		}
	} else if dataType == gdbi.EdgeData {
		type edgeResult struct {
			idx int
			ee  gdbi.Row
		}
		loadedEdges := make([]edgeResult, 0, len(travelers))
		for i, t := range travelers {
			ee := t.GetCurrent()
			if ee == nil {
				continue
			}
			loadedEdges = append(loadedEdges, edgeResult{idx: i, ee: ee})
		}

		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		if workers > len(loadedEdges) {
			workers = len(loadedEdges)
		}
		if workers <= 1 {
			for _, item := range loadedEdges {
				if item.ee == nil {
					continue
				}
				if rowNeedsLoad(item.ee) {
					loaded := graph.GetEdge(item.ee.GetID(), true)
					if loaded == nil {
						continue
					}
					item.ee = loaded
				}
				results[item.idx] = &gripql.QueryResult{
					Result: &gripql.QueryResult_Edge{
						Edge: gdbi.RowToEdge(item.ee),
					},
				}
			}
		} else {
			jobs := make(chan edgeResult, workers*2)
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for item := range jobs {
						if item.ee == nil {
							continue
						}
						ee := item.ee
						if rowNeedsLoad(ee) {
							loaded := graph.GetEdge(ee.GetID(), true)
							if loaded == nil {
								continue
							}
							ee = loaded
						}
						results[item.idx] = &gripql.QueryResult{
							Result: &gripql.QueryResult_Edge{
								Edge: gdbi.RowToEdge(ee),
							},
						}
					}
				}()
			}
			for _, item := range loadedEdges {
				jobs <- item
			}
			close(jobs)
			wg.Wait()
		}
	} else {
		for i, t := range travelers {
			results[i] = Convert(graph, dataType, markTypes, t)
		}
	}
	return results
}
