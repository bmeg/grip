package engine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

type inPipe <-chan *traveler
type outPipe chan<- *traveler

type traveler struct {
	id          string
	label       string
	from, to    string
	data        map[string]interface{}
	marks       map[string]*traveler
	count       int64
	groupCounts map[string]int64
	row         []*traveler
	value       interface{}
	dataType
}

type dataType uint8

const (
	noData dataType = iota
	vertexData
	edgeData
	countData
	groupCountData
	valueData
	rowData
)

func start(procs []processor, bufsize int) <-chan *traveler {
	if len(procs) == 0 {
		ch := make(chan *traveler)
		close(ch)
		return ch
	}

	in := make(chan *traveler)
	final := make(chan *traveler, bufsize)

	// Write an empty traveler to input
	// to trigger the computation.
	go initPipe(in)

	for i := 0; i < len(procs)-1; i++ {
		glue := make(chan *traveler, bufsize)
		go startOne(procs[i], in, glue)
		in = glue
	}

	last := procs[len(procs)-1]
	go startOne(last, in, final)

	return final
}

// Sends an empty traveler to the pipe to kick off pipelines of processors.
func initPipe(out outPipe) {
	out <- &traveler{}
	close(out)
}

func startOne(proc processor, in inPipe, out outPipe) {
	proc.process(in, out)
	close(out)
}

func convert(t *traveler) *aql.ResultRow {
	switch t.dataType {
	case vertexData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Vertex{
					&aql.Vertex{
						Gid:   t.id,
						Label: t.label,
						Data:  protoutil.AsStruct(t.data),
					},
				},
			},
		}

	case edgeData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Edge{
					&aql.Edge{
						Gid:   t.id,
						From:  t.from,
						To:    t.to,
						Label: t.label,
						Data:  protoutil.AsStruct(t.data),
					},
				},
			},
		}

	case countData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.count),
				},
			},
		}

	case groupCountData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.groupCounts),
				},
			},
		}

	case rowData:
		res := &aql.ResultRow{}
		for _, r := range t.row {
			res.Row = append(res.Row, &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(r.data),
				},
			})
		}

		return res

	case valueData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.value),
				},
			},
		}

	default:
		panic(fmt.Errorf("unhandled data type %d", t.dataType))
	}
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}
