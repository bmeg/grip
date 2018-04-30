package aql

import "github.com/bmeg/arachne/protoutil"

// Eq asserts that the value the provided key resolves to is equal to the provided value.
func Eq(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_EQ,
			},
		},
	}
}

// Neq asserts that the value the provided key resolves to is not equal to the provided value.
func Neq(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_NEQ,
			},
		},
	}
}

// Gt asserts that the value the provided key resolves to is greater than the provided value.
func Gt(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_GT,
			},
		},
	}
}

// Gte asserts that the value the provided key resolves to is greater than or equal to the provided value.
func Gte(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_GTE,
			},
		},
	}
}

// Lt asserts that the value the provided key resolves to is less than the provided value.
func Lt(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_LT,
			},
		},
	}
}

// Lte asserts that the value the provided key resolves to is less than or equal to the provided value.
func Lte(key string, value interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_LTE,
			},
		},
	}
}

// In asserts that the value the provided key resolves to is in the provided list of values.
func In(key string, values ...interface{}) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Condition{
			Condition: &WhereCondition{
				Key:       key,
				Value:     protoutil.WrapValue(values),
				Condition: Condition_IN,
			},
		},
	}
}

// And repreesents a logical "and" of two or more WhereExpressions
func And(expressions ...*WhereExpression) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_And{
			And: &WhereExpressionList{expressions},
		},
	}
}

// Or repreesents a logical "or" of two or more WhereExpressions
func Or(expressions ...*WhereExpression) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Or{
			Or: &WhereExpressionList{expressions},
		},
	}
}

// Not repreesents a logical "not" for a WhereExpression
func Not(expression *WhereExpression) *WhereExpression {
	return &WhereExpression{
		Expression: &WhereExpression_Not{
			Not: expression,
		},
	}
}
