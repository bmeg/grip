package mongo

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"go.mongodb.org/mongo-driver/bson"
)

func convertHasExpression(stmt *gripql.HasExpression, not bool) bson.M {
	output := bson.M{}
	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		switch cond.Condition {
		case gripql.Condition_INSIDE:
			val := cond.Value.AsInterface()
			lims, ok := val.([]interface{})
			if !ok || len(lims) < 2 {
				log.Error("unable to cast values from INSIDE statement")
			} else {
				key := cond.Key
				output = convertHasExpression(gripql.And(gripql.Gt(key, lims[0]), gripql.Lt(key, lims[1])), not)
				//fmt.Printf("inside: %#v\n", output)
			}

		case gripql.Condition_OUTSIDE:
			val := cond.Value.AsInterface()
			lims, ok := val.([]interface{})
			if !ok || len(lims) < 2 {
				log.Error("unable to cast values from OUTSIDE statement")
			} else {
				key := cond.Key
				output = convertHasExpression(gripql.Or(gripql.Lt(key, lims[0]), gripql.Gt(key, lims[1])), not)
			}

		case gripql.Condition_BETWEEN:
			val := cond.Value.AsInterface()
			lims, ok := val.([]interface{})
			if !ok || len(lims) < 2 {
				log.Error("unable to cast values from BETWEEN statement")
			} else {
				key := cond.Key
				output = convertHasExpression(gripql.And(gripql.Gte(key, lims[0]), gripql.Lt(key, lims[1])), not)
			}

		default:
			output = convertCondition(cond, not)
		}

	case *gripql.HasExpression_And:
		and := stmt.GetAnd()
		andRes := []bson.M{}
		for _, e := range and.Expressions {
			andRes = append(andRes, convertHasExpression(e, not))
		}
		output = bson.M{"$and": andRes}
		if not {
			output = bson.M{"$or": andRes}
		}

	case *gripql.HasExpression_Or:
		or := stmt.GetOr()
		orRes := []bson.M{}
		for _, e := range or.Expressions {
			orRes = append(orRes, convertHasExpression(e, not))
		}
		output = bson.M{"$or": orRes}
		if not {
			output = bson.M{"$and": orRes}
		}

	case *gripql.HasExpression_Not:
		notRes := convertHasExpression(stmt.GetNot(), true)
		output = notRes
		fmt.Printf("not: %#v\n", output)

	default:
		log.Error("unknown where expression type")
	}

	return output
}

func convertCondition(cond *gripql.HasCondition, not bool) bson.M {
	var key string
	var val interface{}
	key = ToPipelinePath(cond.Key)
	val = cond.Value.AsInterface()

	isExpr := false

	if valStr, ok := val.(string); ok {
		if strings.HasPrefix(valStr, "$") {
			//user has a field reference to compare to, rather then a value
			//we'll need to use the '$expr' and refer to the fields using the '$' prefix
			val = "$" + ToPipelinePath(valStr)
			key = "$" + key
			isExpr = true
		}
		log.Infof("mongo val str: %s(%s) -- %s(%s)", cond.Key, key, valStr, val)
	}
	bCond := bson.M{}
	switch cond.Condition {
	case gripql.Condition_EQ:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$eq": []any{key, val}}}
		} else {
			bCond = bson.M{"$eq": val}
		}
	case gripql.Condition_NEQ:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$ne": []any{key, val}}}
		} else {
			bCond = bson.M{"$ne": val}
		}
	case gripql.Condition_GT:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$gt": []any{key, val}}}
		} else {
			bCond = bson.M{"$gt": val}
		}
	case gripql.Condition_GTE:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$gte": []any{key, val}}}
		} else {
			bCond = bson.M{"$gte": val}
		}
	case gripql.Condition_LT:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$lt": []any{key, val}}}
		} else {
			bCond = bson.M{"$lt": val}
		}
	case gripql.Condition_LTE:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$lte": []any{key, val}}}
		} else {
			bCond = bson.M{"$lte": val}
		}
	case gripql.Condition_WITHIN:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$in": []any{key, val}}}
		} else {
			bCond = bson.M{"$in": val}
		}
	case gripql.Condition_WITHOUT:
		if isExpr {
			bCond = bson.M{"$not": bson.M{"$expr": bson.M{"$in": []any{key, val}}}}
		} else {
			bCond = bson.M{"$not": bson.M{"$in": val}}
		}
	case gripql.Condition_CONTAINS:
		if isExpr {
			bCond = bson.M{"$expr": bson.M{"$in": []any{key, val}}}
		} else {
			bCond = bson.M{"$in": []any{val}}
		}
	default:
		log.Error("unknown where condition type")
	}
	if not {
		return bson.M{key: bson.M{"$not": bCond}}
	}
	if isExpr {
		return bCond
	}
	return bson.M{key: bCond}
}
