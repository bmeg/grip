package engine

import (
	"fmt"
	"github.com/bmeg/grip/gripql"
	"testing"
)


func TestStepNumber(t *testing.T) {
	q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))
	o := PipelineSteps(q.Statements)
	if !arrayEq(o, []string{"1", "2", "3", "3"}) {
		t.Error("Step mapping error")
	}
}

func TestAsMapping(t *testing.T) {
  q := gripql.NewQuery()
	q = q.V().As("a").Out().As("b").In()
	out := PipelineAsSteps(q.Statements)
  fmt.Printf("vars: %s\n", out)
}

func TestOutputMasking(t *testing.T) {
  q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))
	out := PipelineStepOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)
	if len(out) != 1 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
	if !arrayEq(out["3"], []string{}) {
		t.Errorf("Incorrect output")
	}

  q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Out()
	out = PipelineStepOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)
	if len(out) != 2 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
	if !arrayEq(out["3"], []string{}) {
		t.Errorf("Incorrect output")
	}
	if !arrayEq(out["4"], []string{}) {
		t.Errorf("Incorrect output")
	}

  q = gripql.NewQuery()
  q = q.V().Out().In().Count()
  out = PipelineStepOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = PipelineStepOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = PipelineStepOutputs(q.Statements)
	if len(out) != 2 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
  fmt.Printf("outputs: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().As("a").Out().Out().Select("a")
	out = PipelineStepOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

}


func TestPathFind(t *testing.T) {
	q := gripql.NewQuery()
	o := q.V().HasLabel("test").Out().As("a").Out().Out().Select("a")
	r := PipelineNoLoadPathSteps(o.Statements)
	fmt.Printf("%s\n", r)

	q = gripql.NewQuery()
	o = q.V().HasLabel("test").Out().Out().Out().In("test")
	r = PipelineNoLoadPathSteps(o.Statements)
	fmt.Printf("%s\n", r)
}
