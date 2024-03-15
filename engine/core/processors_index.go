package core

import (
	"context"

	"github.com/bmeg/grip/gdbi"
)

////////////////////////////////////////////////////////////////////////////////

// LookupVertsIndex look up vertices by indexed based feature
type LookupVertsIndex struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process LookupVertsIndex
func (l *LookupVertsIndex) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			for _, label := range l.labels {
				for id := range l.db.Index().VertexLabelScan(ctx, label) {
					queryChan <- gdbi.ElementLookup{
						ID:  id,
						Ref: t,
					}
				}
			}
		}
	}()

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			out <- i.AddCurrent(v.Vertex.Copy())
		}
	}()
	return ctx
}
