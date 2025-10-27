package grids

import (
	bFilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/table"
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

func (f *GripQLFilter) Matches(row []byte, tableName string) bool {
	return MatchesHasExpression(row, f.Expression, tableName)
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

func MatchesHasExpression(row []byte, stmt *gripql.HasExpression, tableName string) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		var lookupVal any
		if cond.Key == "_label" {
			lookupVal = tableName[2:]
		} else if cond.Key == "_id" {
			node, err := sonic.Get(row, []any{"1"}...)
			if err != nil {
				if err != ast.ErrNotExist {
					log.Errorf("Sonic Fetch err for path 1 on doc %#v: %v", string(row), err)
				}
				return false
			}
			lookupVal, err = node.Interface()
			if err != nil {
				log.Errorf("Error unmarshaling node: %v", err)
				return false
			}
		} else {
			pathArr, err := table.ConvertJSONPathToArray(cond.Key)
			if err != nil {
				log.Errorf("Error converting JSON path: %v", err)
				return false
			}
			node, err := sonic.Get(row, pathArr...)
			if err != nil {
				if err != ast.ErrNotExist {
					log.Errorf("Sonic Fetch err for path: %s on doc %#v: %v", pathArr, string(row), err)
					return false
				}
				lookupVal = nil
			} else {
				lookupVal, err = node.Interface()
				if err != nil {
					log.Errorf("Error unmarshaling node: %v", err)
					return false
				}
			}
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
		for _, e := range stmt.GetAnd().Expressions {
			if !MatchesHasExpression(row, e, tableName) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if MatchesHasExpression(row, e, tableName) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !MatchesHasExpression(row, stmt.GetNot(), tableName)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}
