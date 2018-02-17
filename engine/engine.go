package engine

import (
	"context"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
)

func Start(ctx context.Context, stmts []*aql.GraphStatement, db gdbi.GraphDB) (<-chan *aql.ResultRow, error) {

	procs, err := compile(stmts, db)
	if err != nil {
		return nil, err
	}

	bufsize := 100
	resch := make(chan *aql.ResultRow, bufsize)

	go func() {
		defer close(resch)

		for t := range start(procs, bufsize) {
			resch <- convert(t)
		}
	}()

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
