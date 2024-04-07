package inspect

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

// Determine if a pipeline starts with 'V().hasLabel(x)' and trim it out
// This can be used to optimize pipelines that start with looking up vertex labels
// using an index
func FindVertexHasLabelStart(pipe []*gripql.GraphStatement) ([]string, []*gripql.GraphStatement) {
	hasLabelLen := 0
	labels := []string{}
	isDone := false
	for i, step := range pipe {
		if isDone {
			break
		}
		if i == 0 {
			if _, ok := step.GetStatement().(*gripql.GraphStatement_V); ok {
				//lookupV = lv
			} else {
				break
			}
			continue
		}
		switch s := step.GetStatement().(type) {
		case *gripql.GraphStatement_HasLabel:
			labels = protoutil.AsStringList(s.HasLabel)
			hasLabelLen = i + 1
		default:
			isDone = true
		}
	}
	return labels, pipe[hasLabelLen:]
}

func FindEdgeHasLabelStart(pipe []*gripql.GraphStatement) ([]string, []*gripql.GraphStatement) {
	hasLabelLen := 0
	labels := []string{}
	isDone := false
	for i, step := range pipe {
		if isDone {
			break
		}
		if i == 0 {
			if _, ok := step.GetStatement().(*gripql.GraphStatement_E); ok {
			} else {
				break
			}
			continue
		}
		switch s := step.GetStatement().(type) {
		case *gripql.GraphStatement_HasLabel:
			labels = protoutil.AsStringList(s.HasLabel)
			hasLabelLen = i + 1
		default:
			isDone = true
		}
	}
	return labels, pipe[hasLabelLen:]
}
