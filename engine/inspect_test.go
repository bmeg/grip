package engine

import (
	"fmt"
	"github.com/bmeg/grip/gripql"
	"testing"
)

func arrayEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}


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
	out := PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Out()
	out = PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
  q = q.V().Out().In().Count()
  out = PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

  q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().As("a").Out().Out().Select("a")
	out = PipelineOutputs(q.Statements)
  fmt.Printf("vars: %s\n", out)

}
