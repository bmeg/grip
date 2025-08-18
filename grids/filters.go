package grids

import (
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/bsontable/filters"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
)

type Filter interface {
	Matches(val []byte) bool
}

// ConditionFilter is a precompiled condition filter
type ConditionFilter struct {
	Cond         *gripql.HasExpression
	PathElements []interface{}
	WcIdx        int
	Operator     benchtop.OperatorType
	Value        filters.FilterValue
}


func (f *ConditionFilter) Matches(val []byte) bool {
	if f.WcIdx == -1 {
		node, err := sonic.Get(val, f.PathElements...)
		if err != nil || node.TypeSafe() == ast.V_NONE {
			return false
		}
		return filters.ApplyFilterCondition(&node, &benchtop.FieldFilter{
			Operator: f.Operator,
			Value:    f.Value,
		})
	}
	node, err := sonic.Get(val, f.PathElements[:f.WcIdx]...)
	if err != nil || node.TypeSafe() == ast.V_NONE {
		return false
	}
	return evaluatePathWithWildcards(&node, f.PathElements[f.WcIdx:], &benchtop.FieldFilter{
		Operator: f.Operator,
		Value:    f.Value,
	})
}

// AndFilter combines multiple filters with AND logic
type AndFilter struct {
	Filters []Filter
}

// Matches returns true if all sub-filters match
func (f *AndFilter) Matches(val []byte) bool {
	for _, filter := range f.Filters {
		if !filter.Matches(val) {
			return false
		}
	}
	return true
}

// OrFilter combines multiple filters with OR logic
type OrFilter struct {
	Filters []Filter
}

// Matches returns true if any sub-filter matches
func (f *OrFilter) Matches(val []byte) bool {
	for _, filter := range f.Filters {
		if filter.Matches(val) {
			return true
		}
	}
	return false
}

// NotFilter inverts a filter
type NotFilter struct {
	Filter Filter
}

// Matches returns true if the sub-filter does not match
func (f *NotFilter) Matches(val []byte) bool {
	return !f.Filter.Matches(val)
}

// GripQLFilter wraps a precompiled filter
type GripQLFilter struct {
    filter Filter
    keys []string
}

// NewGripQLFilter creates a new filter from a HasExpression
func NewGripQLFilter(expr *gripql.HasExpression) *GripQLFilter {
    return &GripQLFilter{
        filter: CompileHasExpression(expr),
        keys: extractKeys(expr),
    }
}


func (f *GripQLFilter) RequiredFields() []string {
	return f.keys
}

func (f *GripQLFilter) IsNoOp() bool {
	// A GripQLFilter is a no-op if its Expression is nil
	return f.filter == nil
}

// Matches checks if a row matches the filter
func (f *GripQLFilter) Matches(val any) bool {
    if f.filter == nil {
        return true
    }
    switch v := val.(type) {
    case []byte:
        return f.filter.Matches(v)
    case map[string]interface{}:
        // Handle simple conditions directly
        if cf, ok := f.filter.(*ConditionFilter); ok && cf.WcIdx == -1 {
            lookupVal := bsontable.PathLookup(v, cf.Cond.GetCondition().Key)
            return filters.ApplyBaseFilterCondition(lookupVal, &benchtop.FieldFilter{
                Operator: cf.Operator,
                Value:    cf.Value,
            })
        }

        return false
    default:
        log.Errorf("Unsupported input type: %T", val)
        return false
    }
}


// CompileHasExpression builds a Filter from a HasExpression
func CompileHasExpression(expr *gripql.HasExpression) Filter {
	if expr == nil {
		return nil
	}
	switch e := expr.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := e.Condition
		pathElements, wcIdx, err := bsontable.ParseJSONPath(cond.Key)
		if err != nil {
			log.Errorf("Error parsing JSON path '%s': %v", cond.Key, err)
			return nil
		}
		return &ConditionFilter{
			Cond:         expr,
			PathElements: pathElements,
			WcIdx:        wcIdx,
			Operator:     MapConditionToOperator(cond.Condition),
			Value:        filters.NewFilterValue(cond.Value.AsInterface()),
		}
	case *gripql.HasExpression_And:
		filters := make([]Filter, len(e.And.Expressions))
		for i, subExpr := range e.And.Expressions {
			filters[i] = CompileHasExpression(subExpr)
		}
		return &AndFilter{Filters: filters}
	case *gripql.HasExpression_Or:
		filters := make([]Filter, len(e.Or.Expressions))
		for i, subExpr := range e.Or.Expressions {
			filters[i] = CompileHasExpression(subExpr)
		}
		return &OrFilter{Filters: filters}
	case *gripql.HasExpression_Not:
		return &NotFilter{Filter: CompileHasExpression(e.Not)}
	default:
		log.Errorf("Unknown expression type: %T", expr.Expression)
		return nil
	}
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

/*
// MatchesHasExpression evaluates if a value matches a HasExpression statement
func MatchesHasExpression(val any, stmt *gripql.HasExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		filter := &benchtop.FieldFilter{
			Operator: MapConditionToOperator(cond.Condition),
			Value:    filters.NewFilterValue(cond.Value.AsInterface()),
		}

		switch v := val.(type) {
		case []byte:
			pathElements, wcIdx, err := bsontable.ParseJSONPath(cond.Key)
			if err != nil {
				log.Infof("Error parsing JSON path '%s': %v", cond.Key, err)
				return false
			}
			// Use sonic.Get for prefix path traversal up to wildcard

			if wcIdx == -1 {
				node, err := sonic.Get(v, pathElements...)
				if err != nil || node.TypeSafe() == ast.V_NONE {
					return false
				}
				return filters.ApplyFilterCondition(&node, filter)
			}
			node, err := sonic.Get(v, pathElements[:wcIdx]...)
			if err != nil || node.TypeSafe() == ast.V_NONE {
				return false
			}
			// Pass remaining path elements after prefix
			return evaluatePathWithWildcards(&node, pathElements[wcIdx:], filter)
		case map[string]interface{}:
			lookupVal := bsontable.PathLookup(v, cond.Key)
			return filters.ApplyBaseFilterCondition(lookupVal, filter)
		default:
			log.Errorf("Unsupported input type for HasExpression condition: %T", val)
			return false
		}

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !MatchesHasExpression(val, e) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if MatchesHasExpression(val, e) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !MatchesHasExpression(val, stmt.GetNot())

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}
*/

func MapConditionToOperator(condition gripql.Condition) benchtop.OperatorType {
	switch condition {
	case gripql.Condition_EQ:
		return benchtop.OP_EQ
	case gripql.Condition_NEQ:
		return benchtop.OP_NEQ
	case gripql.Condition_GT:
		return benchtop.OP_GT
	case gripql.Condition_GTE:
		return benchtop.OP_GTE
	case gripql.Condition_LT:
		return benchtop.OP_LT
	case gripql.Condition_LTE:
		return benchtop.OP_LTE
	case gripql.Condition_INSIDE:
		return benchtop.OP_INSIDE
	case gripql.Condition_OUTSIDE:
		return benchtop.OP_OUTSIDE
	case gripql.Condition_BETWEEN:
		return benchtop.OP_BETWEEN
	case gripql.Condition_WITHIN:
		return benchtop.OP_WITHIN
	case gripql.Condition_WITHOUT:
		return benchtop.OP_WITHOUT
	case gripql.Condition_CONTAINS:
		return benchtop.OP_CONTAINS
	default:
		// For Condition_UNKNOWN_CONDITION or any other unmapped value,
		// return an empty string or a specific "UNKNOWN" operator type if preferred.
		return ""
	}
}

func evaluatePathWithWildcards(node *ast.Node, pathElements []interface{}, filter *benchtop.FieldFilter) bool {
	if node.TypeSafe() == ast.V_NONE {
		return false
	}

	i := 0
	currentNode := node
	for i < len(pathElements) && pathElements[i] != "*" {
		var nextNode *ast.Node
		switch elem := pathElements[i].(type) {
		case int:
			if currentNode.TypeSafe() != ast.V_ARRAY {
				return false
			}
			nextNode = currentNode.Index(elem)
		case string:
			if currentNode.TypeSafe() != ast.V_OBJECT {
				return false
			}
			nextNode = currentNode.Get(elem)
		}
		if nextNode == nil || nextNode.TypeSafe() == ast.V_NONE {
			return false
		}
		currentNode = nextNode
		i++
	}

	if i == len(pathElements) {
		return filters.ApplyFilterCondition(currentNode, filter)
	}

	if currentNode.TypeSafe() != ast.V_ARRAY {
		return false
	}
	for j := 0; ; j++ {
		childNode := currentNode.Index(j)
		if childNode == nil {
			break
		}
		if childNode.TypeSafe() == ast.V_NONE {
			continue
		}
		if evaluatePathWithWildcards(childNode, pathElements[i+1:], filter) {
			return true
		}
	}
	return false
}
