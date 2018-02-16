package engine

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
)

func Start(ctx context.Context, stmts []*aql.GraphStatement, db gdbi.GraphDB) (<-chan *aql.ResultRow, error) {

	procs, err := compile(stmts, db)
	if err != nil {
		return nil, err
	}

	bufsize := 100
	resch := make(chan *aql.ResultRow, bufsize)

	for t := range start(procs, bufsize) {
		switch t.dataType {
		case vertexData:
			resch <- &aql.ResultRow{
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
			resch <- &aql.ResultRow{
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
			resch <- &aql.ResultRow{
				Value: &aql.QueryResult{
					// TODO loss of precision. counts should be int64
					&aql.QueryResult_IntValue{int32(t.count)},
				},
			}
		case groupCountData:
			resch <- &aql.ResultRow{
				Value: &aql.QueryResult{
					&aql.QueryResult_Struct{},
				},
			}
		case rowData:
			res := &aql.ResultRow{}
			for _, r := range t.row {
				res.Row = append(res.Row, &aql.QueryResult{
					&aql.QueryResult_Struct{
						protoutil.AsStruct(r.data),
					},
				})
			}

			resch <- res
		case valueData:
			resch <- &aql.ResultRow{
				Value: &aql.QueryResult{
					// TODO wrong
					&aql.QueryResult_Struct{},
				},
			}
		case noData:
		default:
			panic(fmt.Errorf("unhandled data type %d", t.dataType))
		}
	}

	return resch, nil
}

func Run(ctx context.Context, stmts []*aql.GraphStatement, db gdbi.GraphDB) ([]*aql.ResultRow, error) {
	ch, err := Start(ctx, stmts, db)
	if err != nil {
		return nil, err
	}

	res := []*aql.ResultRow{}
	for t := range ch {
		res = append(res, t)
	}
	return res, nil
}
