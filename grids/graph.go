package grids

import (
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
)

func labelFromElementID(id string) string {
	if i := strings.IndexByte(id, ':'); i > 0 {
		return id[:i]
	}
	return ""
}

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	return ggraph.ts.Get(ggraph.graphID)
}

func (ggraph *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(ggraph, GridsOptimizer, core.IndexStartOptimize)
}
