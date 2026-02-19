package filter

import (
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/gripql"
)

// ToQueryCondition converts a GripQL condition to a Bencthop query condition
func ToQueryCondition(c gripql.Condition) query.Condition {
	switch c {
	case gripql.Condition_EQ:
		return query.EQ
	case gripql.Condition_NEQ:
		return query.NEQ
	case gripql.Condition_GT:
		return query.GT
	case gripql.Condition_GTE:
		return query.GTE
	case gripql.Condition_LT:
		return query.LT
	case gripql.Condition_LTE:
		return query.LTE
	case gripql.Condition_INSIDE:
		return query.INSIDE
	case gripql.Condition_OUTSIDE:
		return query.OUTSIDE
	case gripql.Condition_BETWEEN:
		return query.BETWEEN
	case gripql.Condition_WITHIN:
		return query.WITHIN
	case gripql.Condition_WITHOUT:
		return query.WITHOUT
	case gripql.Condition_CONTAINS:
		return query.CONTAINS
	default:
		return query.EQ
	}
}
