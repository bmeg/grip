package grids

import (
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

type OptimizationRule struct {
	Match   func(pipe []*gripql.GraphStatement) bool
	Replace func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement
}

func GridsOptimizer(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	// Preprocess to handle Has with And expressions
	//pipe = expandHasAnd(pipe)
	// Apply the first matching optimization rule
	for _, rule := range startOptimizations {
		if rule.Match(pipe) {
			return rule.Replace(pipe)
		}
	}
	// Return the original pipeline if no optimizations apply
	return pipe
}

var startOptimizations = []OptimizationRule{
	{
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 2 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if _, ok := pipe[1].GetStatement().(*gripql.GraphStatement_Has); ok {
				return true
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[1].GetHas()
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V.Has() indexing",
						Custom: lookupVertsCondIndexStep{
							expr: has,
						},
					},
				},
			}
			return append(optimized, pipe[2:]...)
		},
	},
	{
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 3 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if _, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); !ok {
				return false
			}
			if stmt, ok := pipe[2].GetStatement().(*gripql.GraphStatement_Has); ok {
				if stmt.Has.GetCondition() == nil {
					return false
				}
				return true
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[2].GetHas()
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			for i, label := range labels {
				if label[:2] != key.VertexTablePrefix {
					labels[i] = key.VertexTablePrefix + label
				}
			}
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V().HasLabel().Has() indexing",
						Custom: lookupVertsHasLabelCondIndexStep{
							expr:   has,
							labels: labels,
						},
					},
				},
			}
			return append(optimized, pipe[3:]...)
		},
	},
	{
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 2 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if _, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); ok {
				return true
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			for i, label := range labels {
				if label[:2] != key.VertexTablePrefix {
					labels[i] = key.VertexTablePrefix + label
				}
			}
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V().HasLabel()",
						Custom: lookupVertsHasLabelCondIndexStep{
							expr:   nil,
							labels: labels,
						},
					},
				},
			}
			return append(optimized, pipe[2:]...)
		},
	},
}
