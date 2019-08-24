package engine

import (
	"fmt"
	"github.com/bmeg/grip/gripql"
)

func PipelineSteps(stmts []*gripql.GraphStatement) []string {
	out := []string{}
	curState := 0
	for _, gs := range stmts {
		switch gs.GetStatement().(type) {
		case *gripql.GraphStatement_V, *gripql.GraphStatement_Out, *gripql.GraphStatement_In,
			*gripql.GraphStatement_OutE, *gripql.GraphStatement_InE, *gripql.GraphStatement_Both,
			*gripql.GraphStatement_BothE, *gripql.GraphStatement_Select:
			curState += 1
		}
		out = append(out, fmt.Sprintf("%d", curState))
	}
	return out
}

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

func PipelineOutputs(stmts []*gripql.GraphStatement) map[string][]string {

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
