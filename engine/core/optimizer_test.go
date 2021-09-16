package core

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
	"github.com/davecgh/go-spew/spew"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestIndexStartOptimize(t *testing.T) {
	expected := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}
	original := []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
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
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Out{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	value123 := structpb.NewListValue(protoutil.NewListFromStrings([]string{"1", "2", "3"}))
	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     value123,
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

	value1, _ := structpb.NewValue(1)
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_gid",
					Value:     value1,
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
					Value:     value1,
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

	valueBar, _ := structpb.NewValue("bar")
	// order shouldnt matter
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     valueBar,
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
					Value:     valueBar,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     value123,
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

	value45 := structpb.NewListValue(protoutil.NewListFromStrings([]string{"4", "5"}))
	// only use the first statement
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     value45,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasId{HasId: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     value45,
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
		{Statement: &gripql.GraphStatement_HasLabel{HasLabel: protoutil.NewListFromStrings([]string{"foo", "bar"})}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	optimized = IndexStartOptimize(original)
	if !reflect.DeepEqual(optimized, expected) {
		t.Log("actual", spew.Sdump(optimized))
		t.Log("expected:", spew.Sdump(expected))
		t.Error("indexStartOptimize returned an unexpected result")
	}

	fooValue, _ := structpb.NewValue("foo")
	// TODO figure out how to optimize
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_NEQ,
					Key:       "_label",
					Value:     fooValue,
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
					Value:     fooValue,
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

	fooBarValue := structpb.NewListValue(protoutil.NewListFromStrings([]string{"foo", "bar"}))
	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     fooBarValue,
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

	barValue, _ := structpb.NewValue("bar")
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     barValue,
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
					Value:     barValue,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     fooBarValue,
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

	bazValue, _ := structpb.NewValue("baz")
	expected = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_LookupVertsIndex{Labels: []string{"foo", "bar"}}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "_label",
					Value:     bazValue,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Out{}},
	}

	original = []*gripql.GraphStatement{
		{Statement: &gripql.GraphStatement_V{}},
		{Statement: &gripql.GraphStatement_HasLabel{HasLabel: protoutil.NewListFromStrings([]string{"foo", "bar"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "_label",
					Value:     bazValue,
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
		{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{"1", "2", "3"})}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_EQ,
					Key:       "$.data.foo",
					Value:     barValue,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     fooBarValue,
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
					Value:     barValue,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_label",
					Value:     fooBarValue,
				},
			}},
		}},
		{Statement: &gripql.GraphStatement_Has{
			Has: &gripql.HasExpression{Expression: &gripql.HasExpression_Condition{
				Condition: &gripql.HasCondition{
					Condition: gripql.Condition_WITHIN,
					Key:       "_gid",
					Value:     value123,
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
					Value:     barValue,
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
							Value:     barValue,
						}}},
						{Expression: &gripql.HasExpression_Condition{Condition: &gripql.HasCondition{
							Condition: gripql.Condition_WITHIN,
							Key:       "_label",
							Value:     fooBarValue,
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
