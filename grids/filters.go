package grids

import (
	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/gripql"
)

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
