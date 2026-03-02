package logic

import (
	"strings"

	"github.com/spf13/cast"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

func isNumeric(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func equalValue(a any, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	if af, ok := isNumeric(a); ok {
		if bf, ok := isNumeric(b); ok {
			return af == bf
		}
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !equalValue(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !equalValue(v, bv[k]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func toAnySlice(v any) ([]any, bool) {
	switch vals := v.(type) {
	case []any:
		return vals, true
	case []string:
		out := make([]any, len(vals))
		for i := range vals {
			out[i] = vals[i]
		}
		return out, true
	default:
		return nil, false
	}
}

func matchesConditionValue(val any, condVal any, condType gripql.Condition) bool {
	//If filtering on nil or no match was found on float64 casting operators return false
	if (val == nil || condVal == nil) &&
		condType != gripql.Condition_EQ &&
		condType != gripql.Condition_NEQ &&
		condType != gripql.Condition_WITHIN &&
		condType != gripql.Condition_WITHOUT &&
		condType != gripql.Condition_CONTAINS {
		return false
	}

	switch condType {
	case gripql.Condition_EQ:
		return equalValue(val, condVal)

	case gripql.Condition_NEQ:
		return !equalValue(val, condVal)

	case gripql.Condition_GT:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN > condN

	case gripql.Condition_GTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN >= condN

	case gripql.Condition_LT:
		valN, err := cast.ToFloat64E(val)
		//log.Debugf("CAST: ", valN, "ERROR: ", err)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN < condN

	case gripql.Condition_LTE:
		valN, err := cast.ToFloat64E(val)
		if err != nil {
			return false
		}
		condN, err := cast.ToFloat64E(condVal)
		if err != nil {
			return false
		}
		return valN <= condN

	case gripql.Condition_INSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast INSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for INSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower INSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper INSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast INSIDE value: %v", err)
			return false
		}
		return valF > lower && valF < upper

	case gripql.Condition_OUTSIDE:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast OUTSIDE condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for OUTSIDE condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower OUTSIDE condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper OUTSIDE condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast OUTSIDE value: %v", err)
			return false
		}
		return valF < lower || valF > upper

	case gripql.Condition_BETWEEN:
		vals, err := cast.ToSliceE(condVal)
		if err != nil {
			log.Debugf("UserError: could not cast BETWEEN condition value: %v", err)
			return false
		}
		if len(vals) != 2 {
			log.Debugf("UserError: expected slice of length 2 not %v for BETWEEN condition value", len(vals))
			return false
		}
		lower, err := cast.ToFloat64E(vals[0])
		if err != nil {
			log.Debugf("UserError: could not cast lower BETWEEN condition value: %v", err)
			return false
		}
		upper, err := cast.ToFloat64E(vals[1])
		if err != nil {
			log.Debugf("UserError: could not cast upper BETWEEN condition value: %v", err)
			return false
		}
		valF, err := cast.ToFloat64E(val)
		if err != nil {
			log.Debugf("UserError: could not cast BETWEEN value: %v", err)
			return false
		}
		return valF >= lower && valF < upper

	case gripql.Condition_WITHIN:
		found := false
		switch condVal := condVal.(type) {
		case []any:
			for _, v := range condVal {
				if equalValue(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: expected slice not %T for WITHIN condition value", condVal)
		}

		return found

	case gripql.Condition_WITHOUT:
		found := false
		switch condVal := condVal.(type) {
		case []any:
			for _, v := range condVal {
				if equalValue(val, v) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: expected slice not %T for WITHOUT condition value", condVal)

		}

		return !found

	case gripql.Condition_CONTAINS:
		found := false
		switch val := val.(type) {
		case []any:
			for _, v := range val {
				if equalValue(v, condVal) {
					found = true
				}
			}

		case nil:
			found = false

		default:
			log.Debugf("UserError: unknown condition value type %T for CONTAINS condition", val)
		}

		return found

	default:
		return false
	}
}

func MatchesCondition(trav gdbi.Traveler, cond *gripql.HasCondition) bool {
	var val any
	var condVal any

	val = gdbi.TravelerPathLookup(trav, cond.Key)
	condVal = cond.Value.AsInterface()

	if condValStr, ok := condVal.(string); ok {
		if strings.HasPrefix(condValStr, "$.") {
			//log.Infof("condVal: %s\n", condValStr)
			condVal = gdbi.TravelerPathLookup(trav, condValStr)
		}
		//TODO: Add escape for $ user string
	}

	//log.Debugf("match: %s %s %s", condVal, val, cond.Key)
	if vals, ok := toAnySlice(val); ok && cond.Condition != gripql.Condition_CONTAINS {
		if len(vals) == 0 {
			return false
		}
		switch cond.Condition {
		case gripql.Condition_NEQ, gripql.Condition_WITHOUT:
			for _, item := range vals {
				if !matchesConditionValue(item, condVal, cond.Condition) {
					return false
				}
			}
			return true
		default:
			for _, item := range vals {
				if matchesConditionValue(item, condVal, cond.Condition) {
					return true
				}
			}
			return false
		}
	}

	return matchesConditionValue(val, condVal, cond.Condition)
}

func MatchesHasExpression(trav gdbi.Traveler, stmt *gripql.HasExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		return MatchesCondition(trav, cond)

	case *gripql.HasExpression_And:
		and := stmt.GetAnd()
		andRes := []bool{}
		for _, e := range and.Expressions {
			andRes = append(andRes, MatchesHasExpression(trav, e))
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
			orRes = append(orRes, MatchesHasExpression(trav, e))
		}
		for _, r := range orRes {
			if r {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		e := stmt.GetNot()
		return !MatchesHasExpression(trav, e)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}
