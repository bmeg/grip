package core

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// Sort rows
type Sort struct {
	sortFields []*gripql.SortField
}

// Process runs LookupEdges
func (s *Sort) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			out <- t
		}
	}()

	return ctx
}
