package inspect

import (
	"fmt"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"

	"github.com/bmeg/grip/util/protoutil"
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

// PipelineSteps create an array, the same length at stmts that labels the
// step id for each of the GraphStatements
func PipelineSteps(stmts []*gripql.GraphStatement) []string {
	out := []string{}
	curState := 0
	for _, gs := range stmts {
		switch gs.GetStatement().(type) {
		//These commands all change the position of the traveler. When that happens,
		//we go to the next 'step' of the traversal
		case *gripql.GraphStatement_V, *gripql.GraphStatement_E, *gripql.GraphStatement_Out,
			*gripql.GraphStatement_In, *gripql.GraphStatement_OutE, *gripql.GraphStatement_InE,
			*gripql.GraphStatement_Both, *gripql.GraphStatement_BothE, *gripql.GraphStatement_Select,
			*gripql.GraphStatement_InNull, *gripql.GraphStatement_OutNull,
			*gripql.GraphStatement_InENull, *gripql.GraphStatement_OutENull:
			curState++
		case *gripql.GraphStatement_Limit, *gripql.GraphStatement_As, *gripql.GraphStatement_Has,
			*gripql.GraphStatement_HasId, *gripql.GraphStatement_HasKey, *gripql.GraphStatement_HasLabel,
			*gripql.GraphStatement_Count, *gripql.GraphStatement_Skip, *gripql.GraphStatement_Distinct,
			*gripql.GraphStatement_Range, *gripql.GraphStatement_Aggregate, *gripql.GraphStatement_Render,
			*gripql.GraphStatement_Fields, *gripql.GraphStatement_Unwind, *gripql.GraphStatement_Path,
			*gripql.GraphStatement_Set, *gripql.GraphStatement_Increment,
			*gripql.GraphStatement_Mark, *gripql.GraphStatement_Jump, *gripql.GraphStatement_Pivot:
		case *gripql.GraphStatement_LookupVertsIndex, *gripql.GraphStatement_EngineCustom:
		default:
			log.Errorf("Unknown Graph Statement: %T", gs.GetStatement())
		}
		out = append(out, fmt.Sprintf("%d", curState))
	}
	return out
}

// PipelineSteps identify the variable names each step can be aliasesed using
// the as_ operation
func PipelineAsSteps(stmts []*gripql.GraphStatement) map[string]string {
	out := map[string]string{}
	steps := PipelineSteps(stmts)

	for i, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *gripql.GraphStatement_As:
			out[stmt.As] = steps[i]
		}
	}
	return out
}

// PipelineStepOutputs identify the required outputs for each step in the traversal
func PipelineStepOutputs(stmts []*gripql.GraphStatement, storeMarks bool) map[string][]string {

	// mapping of what steps of the traversal as used at each stage of the pipeline
	steps := PipelineSteps(stmts)

	asMap := PipelineAsSteps(stmts)
	// we're inpecting the pipeline backwards, the last step is emitted
	onLast := true
	out := map[string][]string{}
	for i := len(stmts) - 1; i >= 0; i-- {
		gs := stmts[i]
		switch gs.GetStatement().(type) {
		case *gripql.GraphStatement_Count:
			//if the pipeline ends with counting, we don't need to load data
			onLast = false
		case *gripql.GraphStatement_Select:
			//if the last step is jumping back to a previous mark
			if onLast {
				sel := gs.GetSelect()
				if a, ok := asMap[sel]; ok {
					out[a] = []string{"*"}
				}
				onLast = false
			}

		case *gripql.GraphStatement_Render:
			// determine every step output that is needed for the render
			val := gs.GetRender().AsInterface()
			names := tpath.GetAllNamespaces(val)
			for _, n := range names {
				if n == tpath.CURRENT {
					out[steps[i]] = []string{"*"}
				}
				if a, ok := asMap[n]; ok {
					out[a] = []string{"*"}
				}
			}
			onLast = false

		case *gripql.GraphStatement_Pivot:
			//TODO: figure out which fields are referenced
			onLast = false

		case *gripql.GraphStatement_Distinct:
			//if there is a distinct step, we need to load data, but only for requested fields
			fields := protoutil.AsStringList(gs.GetDistinct())
			for _, f := range fields {
				n := tpath.GetNamespace(f)
				if n == tpath.CURRENT {
					out[steps[i]] = []string{"*"}
				}
				if a, ok := asMap[n]; ok {
					out[a] = []string{"*"}
				}
			}
		case *gripql.GraphStatement_V, *gripql.GraphStatement_E,
			*gripql.GraphStatement_Out, *gripql.GraphStatement_In,
			*gripql.GraphStatement_OutE, *gripql.GraphStatement_InE,
			*gripql.GraphStatement_Both, *gripql.GraphStatement_BothE:
			if onLast {
				out[steps[i]] = []string{"*"}
			}
			onLast = false
		case *gripql.GraphStatement_LookupVertsIndex:
			if onLast {
				out[steps[i]] = []string{"*"}
			}
			onLast = false

		case *gripql.GraphStatement_HasLabel:
			if x, ok := out[steps[i]]; ok {
				out[steps[i]] = append(x, "_label")
			} else {
				out[steps[i]] = []string{"_label"}
			}
		case *gripql.GraphStatement_Has:
			out[steps[i]] = []string{"*"}
		}
	}
	//if the job is a fragment, the elements marked with as_, they may be needed
	//in followup runs
	if storeMarks {
		for _, v := range asMap {
			out[v] = []string{"*"}
		}
	}
	return out
}

// DEPRECATED : Was used for older version of GRIDS engine
// PipelineNoLoadPath identifies 'paths' which are groups of statements that move
// travelers across multiple steps, and don't require data (other then the label)
// to be loaded
/*
func PipelineNoLoadPath(stmts []*gripql.GraphStatement, minLen int) [][]int {
	out := [][]int{}

	steps := PipelineSteps(stmts)
	outputs := PipelineStepOutputs(stmts)
	curPath := []int{}
	for i := range steps {
		switch stmts[i].GetStatement().(type) {
		case *gripql.GraphStatement_In, *gripql.GraphStatement_Out,
			*gripql.GraphStatement_InE, *gripql.GraphStatement_OutE,
			*gripql.GraphStatement_HasLabel:
			if s, ok := outputs[steps[i]]; !ok {
				curPath = append(curPath, i)
			} else {
				if arrayEq(s, []string{"_label"}) {
					curPath = append(curPath, i)
				} else {
					if len(curPath) >= minLen {
						out = append(out, curPath)
					}
					curPath = []int{}
				}
			}
		}
	}
	if len(curPath) >= minLen {
		out = append(out, curPath)
	}
	return out
}
*/