package gripql

import "github.com/bmeg/grip/protoutil"

// Eq asserts that the value the provided key resolves to is equal to the provided value.
func Eq(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_EQ,
			},
		},
	}
}

// Neq asserts that the value the provided key resolves to is not equal to the provided value.
func Neq(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_NEQ,
			},
		},
	}
}

// Gt asserts that the value the provided key resolves to is greater than the provided value.
func Gt(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_GT,
			},
		},
	}
}

// Gte asserts that the value the provided key resolves to is greater than or equal to the provided value.
func Gte(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_GTE,
			},
		},
	}
}

// Lt asserts that the value the provided key resolves to is less than the provided value.
func Lt(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_LT,
			},
		},
	}
}

// Lte asserts that the value the provided key resolves to is less than or equal to the provided value.
func Lte(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_LTE,
			},
		},
	}
}

// Inside asserts that the number the provided key resolves to is greater than
// the first provided number and less than the second.
func Inside(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_INSIDE,
			},
		},
	}
}

// Outside asserts that the number the provided key resolves to is less than
// the first provided number and greater than the second.
func Outside(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_OUTSIDE,
			},
		},
	}
}

// Between asserts that the number the provided key resolves to is greater than
// or equal to the first provided number and less than the second.
func Between(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_BETWEEN,
			},
		},
	}
}

// Within asserts that the value the provided key resolves to is in the provided list of values.
func Within(key string, values ...interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(values),
				Condition: Condition_WITHIN,
			},
		},
	}
}

// Without asserts that the value the provided key resolves to is not in the provided list of values.
func Without(key string, values ...interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(values),
				Condition: Condition_WITHOUT,
			},
		},
	}
}

// Contains asserts that the array the provided key resolves to contains the provided value.
func Contains(key string, value interface{}) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Condition{
			Condition: &HasCondition{
				Key:       key,
				Value:     protoutil.WrapValue(value),
				Condition: Condition_CONTAINS,
			},
		},
	}
}

// And repreesents a logical "and" of two or more HasExpressions
func And(expressions ...*HasExpression) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_And{
			And: &HasExpressionList{
				Expressions: expressions,
			},
		},
	}
}

// Or repreesents a logical "or" of two or more HasExpressions
func Or(expressions ...*HasExpression) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Or{
			Or: &HasExpressionList{
				Expressions: expressions,
			},
		},
	}
}

// Not repreesents a logical "not" for a HasExpression
func Not(expression *HasExpression) *HasExpression {
	return &HasExpression{
		Expression: &HasExpression_Not{
			Not: expression,
		},
	}
}
