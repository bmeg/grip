package core

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
)

//IndexStartOptimize looks at processor pipeline for queries like
// V().Has(Eq("$.label", "Person")) and V().Has(Eq("$.gid", "1")),
// streamline into a single index lookup
func IndexStartOptimize(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	optimized := []*gripql.GraphStatement{}

	//var lookupV *gripql.GraphStatement_V
	hasIDIdx := []int{}
	hasLabelIdx := []int{}
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
		case *gripql.GraphStatement_HasId:
			hasIDIdx = append(hasIDIdx, i)
		case *gripql.GraphStatement_HasLabel:
			hasLabelIdx = append(hasLabelIdx, i)
		case *gripql.GraphStatement_Has:
			if and := s.Has.GetAnd(); and != nil {
				stmts := and.GetExpressions()
				newPipe := []*gripql.GraphStatement{}
				newPipe = append(newPipe, pipe[:i]...)
				for _, stmt := range stmts {
					newPipe = append(newPipe, &gripql.GraphStatement{Statement: &gripql.GraphStatement_Has{Has: stmt}})
				}
				newPipe = append(newPipe, pipe[i+1:]...)
				return IndexStartOptimize(newPipe)
			}
			if cond := s.Has.GetCondition(); cond != nil {
				path := jsonpath.GetJSONPath(cond.Key)
				switch path {
				case "$.gid":
					hasIDIdx = append(hasIDIdx, i)
				case "$.label":
					hasLabelIdx = append(hasLabelIdx, i)
				default:
					// do nothing
				}
			}
		default:
			isDone = true
		}
	}

	idOpt := false
	if len(hasIDIdx) > 0 {
		ids := []string{}
		idx := hasIDIdx[0]
		if has, ok := pipe[idx].GetStatement().(*gripql.GraphStatement_Has); ok {
			ids = append(ids, extractHasVals(has)...)
		}
		if has, ok := pipe[idx].GetStatement().(*gripql.GraphStatement_HasId); ok {
			ids = append(ids, protoutil.AsStringList(has.HasId)...)
		}
		if len(ids) > 0 {
			idOpt = true
			hIdx := &gripql.GraphStatement_V{V: protoutil.AsListValue(ids)}
			optimized = append(optimized, &gripql.GraphStatement{Statement: hIdx})
		}
	}

	labelOpt := false
	if len(hasLabelIdx) > 0 && !idOpt {
		labels := []string{}
		idx := hasLabelIdx[0]
		if has, ok := pipe[idx].GetStatement().(*gripql.GraphStatement_Has); ok {
			labels = append(labels, extractHasVals(has)...)
		}
		if has, ok := pipe[idx].GetStatement().(*gripql.GraphStatement_HasLabel); ok {
			labels = append(labels, protoutil.AsStringList(has.HasLabel)...)
		}
		if len(labels) > 0 {
			labelOpt = true
			hIdx := &gripql.GraphStatementLookupVertsIndex{Labels: labels}
			optimized = append(optimized, &gripql.GraphStatement{Statement: hIdx})
		}
	}

	for i, step := range pipe {
		if idOpt || labelOpt {
			if i == 0 {
				continue
			}
		} else {
			optimized = append(optimized, step)
		}
		if idOpt {
			if i != hasIDIdx[0] {
				optimized = append(optimized, step)
			}
		}
		if labelOpt {
			if i != hasLabelIdx[0] {
				optimized = append(optimized, step)
			}
		}
	}

	return optimized
}

func extractHasVals(h *gripql.GraphStatement_Has) []string {
	vals := []string{}
	if cond := h.Has.GetCondition(); cond != nil {
		// path := jsonpath.GetJSONPath(cond.Key)
		val := protoutil.UnWrapValue(cond.Value)
		switch cond.Condition {
		case gripql.Condition_EQ:
			if l, ok := val.(string); ok {
				vals = []string{l}
			}
		case gripql.Condition_WITHIN:
			v := val.([]interface{})
			for _, x := range v {
				vals = append(vals, x.(string))
			}
		default:
			// do nothing
		}
	}
	return vals
}
