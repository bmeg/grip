package mongo

import (
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
	"github.com/globalsign/mgo/bson"
)

func convertWhereExpression(stmt *aql.WhereExpression, not bool) bson.M {
	output := bson.M{}
	switch stmt.Expression.(type) {
	case *aql.WhereExpression_Condition:
		cond := stmt.GetCondition()
		output = convertCondition(cond, not)

	case *aql.WhereExpression_And:
		and := stmt.GetAnd()
		andRes := []bson.M{}
		for _, e := range and.Expressions {
			andRes = append(andRes, convertWhereExpression(e, not))
		}
		output = bson.M{"$and": andRes}
		if not {
			output = bson.M{"$or": andRes}
		}

	case *aql.WhereExpression_Or:
		or := stmt.GetOr()
		orRes := []bson.M{}
		for _, e := range or.Expressions {
			orRes = append(orRes, convertWhereExpression(e, not))
		}
		output = bson.M{"$or": orRes}
		if not {
			output = bson.M{"$and": orRes}
		}

	case *aql.WhereExpression_Not:
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

func convertCondition(cond *aql.WhereCondition, not bool) bson.M {
	var key string
	var val interface{}
	key = convertPath(cond.Key)
	val = protoutil.UnWrapValue(cond.Value)
	expr := bson.M{}
	switch cond.Condition {
	case aql.Condition_EQ:
		expr = bson.M{"$eq": val}
	case aql.Condition_NEQ:
		expr = bson.M{"$ne": val}
	case aql.Condition_GT:
		expr = bson.M{"$gt": val}
	case aql.Condition_GTE:
		expr = bson.M{"$gte": val}
	case aql.Condition_LT:
		expr = bson.M{"$lt": val}
	case aql.Condition_LTE:
		expr = bson.M{"$lte": val}
	case aql.Condition_IN:
		expr = bson.M{"$in": val}
	case aql.Condition_CONTAINS:
		expr = bson.M{"$in": []interface{}{val}}
	default:
		log.Printf("unknown where condition type")
	}
	if not {
		return bson.M{key: bson.M{"$not": expr}}
	}
	return bson.M{key: expr}
}
