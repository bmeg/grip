package logic

import (
	"reflect"

	"github.com/spf13/cast"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

func MatchesCondition(trav gdbi.Traveler, cond *gripql.HasCondition) bool {
	var val interface{}
	var condVal interface{}

	val = gdbi.TravelerPathLookup(trav, cond.Key)
	condVal = cond.Value.AsInterface()

	/*  If not looking for nil, but nil is found
	and not trying to do a Boolean operation on non numeric data return false.
	Had to add in bool comparison to pass
	TestEngine/_V_HasLabel_users_Has_details_=_string_value:"\"sex\"=>\"M\""_Count#01
	unit test.
	*/
	log.Debug("val: ", val, "condVal: ", condVal)
	if val == nil && condVal != nil &&
		cond.Condition != gripql.Condition_EQ &&
		cond.Condition != gripql.Condition_NEQ &&
		cond.Condition != gripql.Condition_WITHIN &&
		cond.Condition != gripql.Condition_WITHOUT &&
		cond.Condition != gripql.Condition_CONTAINS {
		return false
	}

	log.Debugf("match: %s %s %s", condVal, val, cond.Key)

	switch cond.Condition {
	case gripql.Condition_EQ:
		return reflect.DeepEqual(val, condVal)

	case gripql.Condition_NEQ:
		return !reflect.DeepEqual(val, condVal)

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
		//log.Debugf("match: %#v %#v %s", condVal, val, cond.Key)
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
		case []interface{}:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
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
		case []interface{}:
			for _, v := range condVal {
				if reflect.DeepEqual(val, v) {
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
		case []interface{}:
			for _, v := range val {
				if reflect.DeepEqual(v, condVal) {
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

func MatchesHasExpression(trav gdbi.Traveler, stmt *gripql.HasExpression) bool {
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		log.Debug("COND: ", cond)
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
