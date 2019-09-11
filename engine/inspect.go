package engine

import (
	"fmt"
	"github.com/bmeg/grip/gripql"
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

func contains(a []string, n string) bool {
	for _, c := range a {
		if c == n {
			return true
		}
	}
	return false
}

//PipelineSteps create an array, the same length at stmts that labels the
//step id for each of the GraphStatements
func PipelineSteps(stmts []*gripql.GraphStatement) []string {
	out := []string{}
	curState := 0
	for _, gs := range stmts {
		switch gs.GetStatement().(type) {
		//These commands all change the postion of the traveler. When that happens,
		//we go to the next 'step' of the traversal
		case *gripql.GraphStatement_V, *gripql.GraphStatement_Out, *gripql.GraphStatement_In,
			*gripql.GraphStatement_OutE, *gripql.GraphStatement_InE, *gripql.GraphStatement_Both,
			*gripql.GraphStatement_BothE, *gripql.GraphStatement_Select:
			curState += 1
		}
		out = append(out, fmt.Sprintf("%d", curState))
	}
	return out
}

//PipelineSteps identify the variable names each step can be aliasesed using
//the as_ operation
func PipelineAsSteps(stmts []*gripql.GraphStatement) map[string]string {
  out := map[string]string{}
  steps := PipelineSteps(stmts)

  for i, gs := range stmts {
    switch stmt := gs.GetStatement().(type) {
    case *gripql.GraphStatement_As:
      out[ stmt.As ] = steps[i]
    }
  }
  return out
}

//PipelineStepOutputs identify the required outputs for each step in the traversal
func PipelineStepOutputs(stmts []*gripql.GraphStatement) map[string][]string {

	steps := PipelineSteps(stmts)
  asMap := PipelineAsSteps(stmts)
	onLast := true
	out := map[string][]string{}
	for i := len(stmts) - 1; i >= 0; i-- {
    gs := stmts[i]
		if _, ok := gs.GetStatement().(*gripql.GraphStatement_Count); ok {
			onLast = false
		}
		if onLast {
			switch gs.GetStatement().(type) {
			case *gripql.GraphStatement_Select:
				sel := gs.GetSelect().Marks
        for _, s := range sel {
          if a, ok := asMap[s]; ok {
            out[a] = []string{}
          }
        }
				onLast = false
			case *gripql.GraphStatement_V, *gripql.GraphStatement_Out, *gripql.GraphStatement_In,
				*gripql.GraphStatement_OutE, *gripql.GraphStatement_InE, *gripql.GraphStatement_Both,
				*gripql.GraphStatement_BothE:
				out[steps[i]] = []string{}
				onLast = false
			}
		} else {
      switch gs.GetStatement().(type) {
      case *gripql.GraphStatement_HasLabel:
        if x, ok := out[steps[i]]; ok {
          out[steps[i]] = append(x, "_label")
        } else {
          out[steps[i]] = []string{"_label"}
        }
      case *gripql.GraphStatement_Has:
        out[steps[i]] = []string{}
      }
    }
	}
	return out
}

func uniqueAppend(a []string, n string) []string {
	if !contains(a, n) {
		return append(a, n)
	}
	return a
}

//PipelinePathSteps identifies 'paths' which are groups of steps that move
//travelers across multiple steps, and don't require data (other then the label)
//to be loaded
func PipelinePathSteps(stmts []*gripql.GraphStatement) [][]string {
	out := [][]string{}

	steps := PipelineSteps(stmts)
	outputs := PipelineStepOutputs(stmts)
	curPath := []string{}
	for i := range steps {
		if s, ok := outputs[steps[i]]; !ok {
			curPath = uniqueAppend(curPath, steps[i])
		} else {
			if arrayEq(s, []string{"_label"}) {
				curPath = uniqueAppend(curPath, steps[i])
			} else {
				if len(curPath) > 1 {
					out = append(out, curPath)
				}
				curPath = []string{}
			}
		}
	}
	if len(curPath) > 1 {
		out = append(out, curPath)
	}
	return out
}
