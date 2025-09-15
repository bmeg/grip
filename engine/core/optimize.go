package core

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

// OptimizationRule defines a structure for matching and replacing query pipeline patterns.
type OptimizationRule struct {
	Match   func(pipe []*gripql.GraphStatement) bool
	Replace func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement
}

func OptimizeHasLabelMatch(pipe []*gripql.GraphStatement) bool {
	if len(pipe) < 2 {
		return false
	}
	if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
		return false
	}
	if hasLabel, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); ok {
		return len(hasLabel.HasLabel.GetValues()) > 0
	}
	return false
}

// startOptimizations is a list of rules to optimize the query pipeline.
var startOptimizations = []OptimizationRule{
	{
		// Matches V().HasId(...)
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 2 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if hasId, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasId); ok {
				return len(hasId.HasId.Values) > 0
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			ids := protoutil.AsStringList(pipe[1].GetHasId())
			optimized := []*gripql.GraphStatement{
				{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings(ids)}},
			}
			return append(optimized, pipe[2:]...)
		},
	},
	{
		// Matches V().HasLabel(...)
		Match: OptimizeHasLabelMatch,
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			optimized := []*gripql.GraphStatement{
				{Statement: &gripql.GraphStatement_LookupVertsLabelIndex{Labels: labels}},
			}
			return append(optimized, pipe[2:]...)
		},
	},
}

// expandHasAnd preprocesses the pipeline to split Has statements with And expressions.
func expandHasAnd(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	expanded := []*gripql.GraphStatement{}
	for _, step := range pipe {
		if has, ok := step.GetStatement().(*gripql.GraphStatement_Has); ok {
			if and := has.Has.GetAnd(); and != nil {
				for _, expr := range and.Expressions {
					expanded = append(expanded, &gripql.GraphStatement{Statement: &gripql.GraphStatement_Has{Has: expr}})
				}
			} else {
				expanded = append(expanded, step)
			}
		} else {
			expanded = append(expanded, step)
		}
	}
	return expanded
}

// IndexStartOptimize applies optimization rules to the query pipeline.
func IndexStartOptimize(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	// Preprocess to handle Has with And expressions
	pipe = expandHasAnd(pipe)
	// Apply the first matching optimization rule
	for _, rule := range startOptimizations {
		if rule.Match(pipe) {
			return rule.Replace(pipe)
		}
	}
	// Return the original pipeline if no optimizations apply
	return pipe
}
