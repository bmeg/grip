package engine

import (
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

// Start begins processing a query pipeline
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
		go pipe.startOne(pipe.procs[i], in, glue)
		in = glue
	}

	last := pipe.procs[len(pipe.procs)-1]
	go pipe.startOne(last, in, final)

	return final
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

// Run starts a pipeline and converts the output to server output structures
func (pipe Pipeline) Run() <-chan *aql.ResultRow {

	bufsize := 100
	resch := make(chan *aql.ResultRow, bufsize)

	go func() {
		defer close(resch)
		for t := range pipe.Start(bufsize) {
			resch <- pipe.Convert(t)
		}
	}()

	return resch
}

// Sends an empty traveler to the pipe to kick off pipelines of processors.
func initPipe(out gdbi.OutPipe) {
	out <- &gdbi.Traveler{}
	close(out)
}

func (pipe Pipeline) startOne(proc Processor, in gdbi.InPipe, out gdbi.OutPipe) {
	man := pipe.NewManager()
	proc.Process(man, in, out)
	man.Cleanup()
	close(out)
}
