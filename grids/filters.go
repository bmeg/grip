package grids

import (
	bFilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
)

type GripQLFilter struct {
	Expression *gripql.HasExpression
}

func (f *GripQLFilter) GetFilter() any {
	return f.Expression
}
func (f *GripQLFilter) IsNoOp() bool {
	// A GripQLFilter is a no-op if its Expression is nil
	return f.Expression == nil
}

func (f *GripQLFilter) Matches(row any) bool {
	return MatchesHasExpression(row, f.Expression)
}

func (f *GripQLFilter) RequiredFields() []string {
	return extractKeys(f.Expression)
}

func extractKeys(expr *gripql.HasExpression) []string {
	keys := map[string]struct{}{}

	var recurse func(*gripql.HasExpression)
	recurse = func(e *gripql.HasExpression) {
		if e == nil {
			return
		}
		switch st := e.Expression.(type) {
		case *gripql.HasExpression_Condition:
			keys[st.Condition.GetKey()] = struct{}{}
		case *gripql.HasExpression_And:
			for _, subExpr := range st.And.GetExpressions() {
				recurse(subExpr)
			}
		case *gripql.HasExpression_Or:
			for _, subExpr := range st.Or.GetExpressions() {
				recurse(subExpr)
			}
		case *gripql.HasExpression_Not:
			recurse(st.Not.GetNot())
		}
	}

	recurse(expr)

	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	return out
}
func MatchesHasExpression(val any, stmt *gripql.HasExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		var lookupVal any

		// Handle lookup based on input type
		switch v := val.(type) {
		case map[string]any:
			lookupVal = jsontable.PathLookup(v, cond.Key)
		case []byte:
			pathArr, err := jsontable.ConvertJSONPathToArray(cond.Key)
			if err != nil {
				log.Errorf("Error converting JSON path: %v", err)
				return false
			}
			node, err := sonic.Get(v, pathArr...)
			if err != nil {
				if err != ast.ErrNotExist {
					log.Errorf("Sonic Fetch err for path: %s on doc %#v: %v", pathArr, string(v), err)
				}
				return false
			}
			lookupVal, err = node.Interface()
			if err != nil {
				log.Errorf("Error unmarshaling node: %v", err)
				return false
			}
		default:
			log.Errorf("Unsupported input type: %T", val)
			return false
		}

		return bFilters.ApplyFilterCondition(
			lookupVal,
			&bFilters.FieldFilter{
				Operator: cond.Condition,
				Field:    cond.Key,
				Value:    cond.Value.AsInterface(),
			},
		)

	case *gripql.HasExpression_And:
		and := stmt.GetAnd()
		andRes := []bool{}
		for _, e := range and.Expressions {
			andRes = append(andRes, MatchesHasExpression(val, e))
		}
		for _, r := range andRes {
			if !r {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		or := stmt.GetOr()
		orRes := []bool{}
		for _, e := range or.Expressions {
			orRes = append(orRes, MatchesHasExpression(val, e))
		}
		for _, r := range orRes {
			if r {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		e := stmt.GetNot()
		return !MatchesHasExpression(val, e)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}
