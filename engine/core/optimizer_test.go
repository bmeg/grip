package core

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/davecgh/go-spew/spew"
)

func TestIndexStartOptimize(t *testing.T) {
	expected := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}
	original := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}
	optimized := IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Out{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.AsListValue([]string{"1", "2", "3"})}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Out{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.AsListValue([]string{"1", "2", "3"})}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     protoutil.WrapValue([]string{"1", "2", "3"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_gid",
					Value:     protoutil.WrapValue("1"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_gid",
					Value:     protoutil.WrapValue("1"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// order shouldnt matter
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     protoutil.WrapValue([]string{"1", "2", "3"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// only use the first statement
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     protoutil.WrapValue([]string{"4", "5"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     protoutil.WrapValue([]string{"4", "5"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasLabel{HasLabel: protoutil.AsListValue([]string{"foo", "bar"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// TODO figure out how to optimize
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_label",
					Value:     protoutil.WrapValue("foo"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_label",
					Value:     protoutil.WrapValue("foo"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     protoutil.WrapValue([]string{"foo", "bar"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     protoutil.WrapValue([]string{"foo", "bar"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "_label",
					Value:     protoutil.WrapValue("baz"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasLabel{HasLabel: protoutil.AsListValue([]string{"foo", "bar"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "_label",
					Value:     protoutil.WrapValue("baz"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// use gid over label to optimize queries
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.AsListValue([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     protoutil.WrapValue([]string{"foo", "bar"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     protoutil.WrapValue([]string{"foo", "bar"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     protoutil.WrapValue([]string{"1", "2", "3"}),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	// handle 'and' statements

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     protoutil.WrapValue("bar"),
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_And{
				And: &gripql.HasExpressionList{
					Expressions: []*gripql.HasExpression{
						{Expression: &gripql.HasExpression_Condition{Condition: &gripql.HasCondition{
							Condition: gripql.Condition_EQ,
							Key:       "$.data.foo",
							Value:     protoutil.WrapValue("bar"),
						}}},
						{Expression: &gripql.HasExpression_Condition{Condition: &gripql.HasCondition{
							Condition: gripql.Condition_WITHIN,
							Key:       "_label",
							Value:     protoutil.WrapValue([]string{"foo", "bar"}),
						}}},
					},
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}
}
