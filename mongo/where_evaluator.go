package mongo

import (
	"log"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
	"github.com/globalsign/mgo/bson"
)

func convertWhereExpression(stmt *gripql.WhereExpression, not bool) bson.M {
	output := bson.M{}
	switch stmt.Expression.(type) {
	case *gripql.WhereExpression_Condition:
		cond := stmt.GetCondition()
		output = convertCondition(cond, not)

	case *gripql.WhereExpression_And:
		and := stmt.GetAnd()
		andRes := []bson.M{}
		for _, e := range and.Expressions {
			andRes = append(andRes, convertWhereExpression(e, not))
		}
		output = bson.M{"$and": andRes}
		if not {
			output = bson.M{"$or": andRes}
		}

	case *gripql.WhereExpression_Or:
		or := stmt.GetOr()
		orRes := []bson.M{}
		for _, e := range or.Expressions {
			orRes = append(orRes, convertWhereExpression(e, not))
		}
		output = bson.M{"$or": orRes}
		if not {
			output = bson.M{"$and": orRes}
		}

	case *gripql.WhereExpression_Not:
		notRes := convertWhereExpression(stmt.GetNot(), true)
		output = notRes

	default:
		log.Printf("unknown where expression type")
	}

	return output
}

func convertPath(key string) string {
	key = jsonpath.GetJSONPath(key)
	key = strings.TrimPrefix(key, "$.")
	if key == "gid" {
		key = "_id"
	}
	return key
}

func convertCondition(cond *gripql.WhereCondition, not bool) bson.M {
	var key string
	var val interface{}
	key = convertPath(cond.Key)
	val = protoutil.UnWrapValue(cond.Value)
	expr := bson.M{}
	switch cond.Condition {
	case gripql.Condition_EQ:
		expr = bson.M{"$eq": val}
	case gripql.Condition_NEQ:
		expr = bson.M{"$ne": val}
	case gripql.Condition_GT:
		expr = bson.M{"$gt": val}
	case gripql.Condition_GTE:
		expr = bson.M{"$gte": val}
	case gripql.Condition_LT:
		expr = bson.M{"$lt": val}
	case gripql.Condition_LTE:
		expr = bson.M{"$lte": val}
	case gripql.Condition_IN:
		expr = bson.M{"$in": val}
	case gripql.Condition_CONTAINS:
		expr = bson.M{"$in": []interface{}{val}}
	default:
		log.Printf("unknown where condition type")
	}
	if not {
		return bson.M{key: bson.M{"$not": expr}}
	}
	return bson.M{key: expr}
}
