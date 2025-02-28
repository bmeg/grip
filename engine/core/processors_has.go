package core

import (
	"context"

	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/setcmp"
)

////////////////////////////////////////////////////////////////////////////////

// Has filters based on data
type Has struct {
	stmt *gripql.HasExpression
}

// Process runs Has
func (w *Has) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}

			if logic.MatchesHasExpression(t, w.stmt) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasLabel filters elements based on their label.
type HasLabel struct {
	labels []string
}

// Process runs Count
func (h *HasLabel) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	labels := dedupStringSlice(h.labels)
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if setcmp.ContainsString(labels, t.GetCurrent().Get().Label) {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasKey filters elements based on whether it has one or more properties.
type HasKey struct {
	keys []string
}

// Process runs Count
func (h *HasKey) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		keys := dedupStringSlice(h.keys)
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			found := true
			for _, key := range keys {
				if !gdbi.TravelerPathExists(t, key) {
					found = false
				}
			}
			if found {
				out <- t
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// HasID filters elements based on their id.
type HasID struct {
	ids []string
}

// Process runs Count
func (h *HasID) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		ids := dedupStringSlice(h.ids)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if setcmp.ContainsString(ids, t.GetCurrentID()) {
				out <- t
			}
		}
	}()
	return ctx
}
