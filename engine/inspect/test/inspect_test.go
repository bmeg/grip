package inspect

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/setcmp"
)

func TestStepNumber(t *testing.T) {
	q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))
	o := inspect.PipelineSteps(q.Statements)
	if !setcmp.ArrayEq(o, []string{"1", "2", "3", "3"}) {
		t.Error("Step mapping error")
	}
}

func TestAsMapping(t *testing.T) {
	q := gripql.NewQuery()
	q = q.V().As("a").Out().As("b").In()
	out := inspect.PipelineAsSteps(q.Statements)
	if x, ok := out["a"]; !ok {
		t.Errorf("a step not found")
	} else {
		if x != "1" {
			t.Errorf("a step numbered incorrectly")
		}
	}
	fmt.Printf("vars: %s\n", out)
}

func TestOutputMasking(t *testing.T) {
	q := gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value"))
	out := inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)
	if len(out) != 1 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
	if !setcmp.ArrayEq(out["3"], []string{"*"}) {
		t.Errorf("Incorrect output")
	}

	q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Out()
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)
	if len(out) != 2 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
	if !setcmp.ArrayEq(out["3"], []string{"*"}) {
		t.Errorf("Incorrect output")
	}
	if !setcmp.ArrayEq(out["4"], []string{"*"}) {
		t.Errorf("Incorrect output")
	}

	q = gripql.NewQuery()
	q = q.E()
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("EdgeList vars: %s\n", out)
	if len(out) != 1 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}

	q = gripql.NewQuery()
	q = q.V().Out().In().Count()
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)
	if len(out) != 0 {
		t.Errorf("Incorrect output count")
	}

	q = gripql.NewQuery()
	q = q.V().Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)
	if len(out) != 1 {
		t.Errorf("Incorrect output count")
	}

	q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().In().Has(gripql.Eq("$.test", "value")).Count()
	out = inspect.PipelineStepOutputs(q.Statements)
	if len(out) != 2 {
		t.Errorf("Wrong number of step outputs %d", len(out))
	}
	fmt.Printf("outputs: %s\n", out)

	q = gripql.NewQuery()
	q = q.V().HasLabel("test").Out().As("a").Out().Out().Select("a")
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)

	q = gripql.NewQuery()
	q = q.V().HasLabel("robot", "person")
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s\n", out)

	q = gripql.NewQuery()
	q = q.V().HasLabel("Person").As("person").Out().Distinct("$person.name")
	out = inspect.PipelineStepOutputs(q.Statements)
	fmt.Printf("vars: %s -> %s\n", inspect.PipelineSteps(q.Statements), out)
	if len(out) != 2 {
		t.Errorf("Incorrect output count")
	}
	if x, ok := out["1"]; ok {
		if !setcmp.ContainsString(x, "_label") {
			t.Errorf("_label not found")
		}
	} else {
		t.Errorf("step 1 missing from outputs")
	}
}

func TestOutputIndexMasking(t *testing.T) {
	q := gripql.NewQuery()
	q = gripql.NewQuery()
	q = q.V().HasLabel("robot", "person")
	smts := core.IndexStartOptimize(q.Statements)

	out := inspect.PipelineStepOutputs(smts)
	fmt.Printf("%#v\n", smts)
	if len(out) == 0 {
		t.Errorf("No outputs found")
	}
	fmt.Printf("vars: %s\n", out)
}

func TestPathFind(t *testing.T) {
	q := gripql.NewQuery()
	o := q.V().HasLabel("test").Out().As("a").Out().Out().Select("a")
	r := inspect.PipelineNoLoadPath(o.Statements, 2)
	fmt.Printf("%#v\n", r)
	if len(r) != 1 {
		t.Errorf("wrong number of paths found")
	} else {
		if !setcmp.ArrayIntEq(r[0], []int{4, 5}) {
			t.Errorf("incorrect path found")
		}
	}

	q = gripql.NewQuery()
	o = q.V().HasLabel("test").Out().Out().Out().In("test")
	r = inspect.PipelineNoLoadPath(o.Statements, 2)
	fmt.Printf("%#v\n", r)
	if len(r) != 1 {
		t.Errorf("wrong number of paths found")
	} else {
		if !setcmp.ArrayIntEq(r[0], []int{1, 2, 3, 4}) {
			t.Errorf("incorrect path found")
		}
	}
}
