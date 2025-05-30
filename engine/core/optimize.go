package core

import (
	"github.com/bmeg/grip/log"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

// IndexStartOptimize looks at processor pipeline for queries like
// V().Has(Eq("$._label", "Person")) and V().Has(Eq("$._id", "1")),
// streamline into a single index lookup
func IndexStartOptimize(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	optimized := []*gripql.GraphStatement{}

	//var lookupV *gripql.GraphStatement_V
	hasIDIdx, hasLabelIdx, hasCondIdx := []int{}, []int{}, []int{}
	isDone := false
	for i, step := range pipe {
		if isDone {
			break
		}
		if i == 0 {
			if v, ok := step.GetStatement().(*gripql.GraphStatement_V); ok {
				if v.V != nil && len(v.V.Values) > 0 {
					break
				}
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
				path := tpath.NormalizePath(cond.Key)
				log.Infof("KEY: %s PATH: %s", cond.Key, path)
				switch path {
				case "$_current._id":
					hasIDIdx = append(hasIDIdx, i)
				case "$_current._label":
					hasLabelIdx = append(hasLabelIdx, i)
				default:
					hasCondIdx = append(hasCondIdx, i)
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
			hIdx := &gripql.GraphStatement_V{V: protoutil.NewListFromStrings(ids)}
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
			hIdx := &gripql.GraphStatement_LookupVertsLabelIndex{Labels: labels}
			optimized = append(optimized, &gripql.GraphStatement{Statement: hIdx})
		}
	}

	hasCondOpt := false
	if len(hasCondIdx) > 0 {
		idx := hasCondIdx[0]
		if has, ok := pipe[idx].GetStatement().(*gripql.GraphStatement_Has); ok {
			cond := has.Has.GetCondition()
			optimized = append(optimized,
				&gripql.GraphStatement{Statement: &gripql.GraphStatement_LookupVertexHasCondIndex{
					Key: cond.Key, Value: cond.GetValue().String(),
				}},
			)
			hasCondOpt = true
			log.Infoln("OPTIMiZED: ", optimized)
		}
	}

	for i, step := range pipe {
		if idOpt || labelOpt || hasCondOpt {
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
		if hasCondOpt {
			if i != hasCondIdx[0] {
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
		val := cond.Value.AsInterface()
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
