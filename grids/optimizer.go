package grids

import (
	"strings"

	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

type OptimizationRule struct {
	Match   func(pipe []*gripql.GraphStatement) bool
	Replace func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement
}

type adjacencyStep struct {
	labels   []string
	inbound  bool
	emitNull bool
}

func parseAdjacencyStep(gs *gripql.GraphStatement) (adjacencyStep, bool) {
	switch stmt := gs.GetStatement().(type) {
	case *gripql.GraphStatement_Out:
		return adjacencyStep{labels: protoutil.AsStringList(stmt.Out)}, true
	case *gripql.GraphStatement_OutNull:
		return adjacencyStep{labels: protoutil.AsStringList(stmt.OutNull), emitNull: true}, true
	case *gripql.GraphStatement_In:
		return adjacencyStep{labels: protoutil.AsStringList(stmt.In), inbound: true}, true
	case *gripql.GraphStatement_InNull:
		return adjacencyStep{labels: protoutil.AsStringList(stmt.InNull), inbound: true, emitNull: true}, true
	default:
		return adjacencyStep{}, false
	}
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
			if len(pipe) < 4 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if _, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); !ok {
				return false
			}
			if stmt, ok := pipe[2].GetStatement().(*gripql.GraphStatement_Has); ok {
				if stmt.Has == nil || stmt.Has.Expression == nil {
					return false
				}
				_, ok := parseAdjacencyStep(pipe[3])
				return ok
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[2].GetHas()
			adj, _ := parseAdjacencyStep(pipe[3])
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			for i, label := range labels {
				if !strings.HasPrefix(label, key.VertexTablePrefix) {
					labels[i] = key.VertexTablePrefix + label
				}
			}
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V().HasLabel().Has().Adj fusion",
						Custom: lookupVertsCondIndexTraverseStep{
							expr:       has,
							labels:     labels,
							edgeLabels: adj.labels,
							inbound:    adj.inbound,
							emitNull:   adj.emitNull,
						},
					},
				},
			}
			return append(optimized, pipe[4:]...)
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
			_, ok := parseAdjacencyStep(pipe[2])
			return ok
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			adj, _ := parseAdjacencyStep(pipe[2])
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			for i, label := range labels {
				if !strings.HasPrefix(label, key.VertexTablePrefix) {
					labels[i] = key.VertexTablePrefix + label
				}
			}
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V().HasLabel().Adj fusion",
						Custom: lookupVertsCondIndexTraverseStep{
							labels:     labels,
							edgeLabels: adj.labels,
							inbound:    adj.inbound,
							emitNull:   adj.emitNull,
						},
					},
				},
			}
			return append(optimized, pipe[3:]...)
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
			if stmt, ok := pipe[1].GetStatement().(*gripql.GraphStatement_Has); ok {
				if stmt.Has == nil || stmt.Has.Expression == nil {
					return false
				}
				_, ok := parseAdjacencyStep(pipe[2])
				return ok
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[1].GetHas()
			adj, _ := parseAdjacencyStep(pipe[2])
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids V().Has().Adj fusion",
						Custom: lookupVertsCondIndexTraverseStep{
							expr:       has,
							edgeLabels: adj.labels,
							inbound:    adj.inbound,
							emitNull:   adj.emitNull,
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
				if stmt.Has == nil || stmt.Has.Expression == nil {
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
				if !strings.HasPrefix(label, key.VertexTablePrefix) {
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
				if !strings.HasPrefix(label, key.VertexTablePrefix) {
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
