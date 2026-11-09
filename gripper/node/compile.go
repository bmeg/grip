package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
)

// Handoff receives a traveler that reached a vertex owned by another source.
// cursor is the index of the step the traveler must continue at.
type Handoff func(t gdbi.Traveler, cursor uint32)

// stepKind classifies a pushable statement.
type stepKind int

const (
	kindStart stepKind = iota
	kindFilter
	kindMark
	kindVertexMove // result is a vertex
	kindEdgeMove   // result is an edge
)

func classify(gs *gripql.GraphStatement) (kind stepKind, fanout bool, err error) {
	switch gs.GetStatement().(type) {
	case *gripql.GraphStatement_V:
		return kindStart, false, nil
	case *gripql.GraphStatement_Has, *gripql.GraphStatement_HasLabel,
		*gripql.GraphStatement_HasKey, *gripql.GraphStatement_HasId:
		return kindFilter, false, nil
	case *gripql.GraphStatement_As:
		return kindMark, false, nil
	case *gripql.GraphStatement_Out:
		return kindVertexMove, false, nil
	case *gripql.GraphStatement_In, *gripql.GraphStatement_Both:
		return kindVertexMove, true, nil
	case *gripql.GraphStatement_OutE:
		return kindEdgeMove, false, nil
	case *gripql.GraphStatement_InE, *gripql.GraphStatement_BothE:
		return kindEdgeMove, true, nil
	}
	return 0, false, fmt.Errorf("statement %T cannot run on a resource node", gs.GetStatement())
}

// ValidateSteps checks that a segment only contains pushable statements, that V
// only appears first, and that has expressions only refer to marks that exist when
// the segment is entered at the given cursor.
func ValidateSteps(steps []*gripql.GraphStatement, cursors []int) error {
	for i, gs := range steps {
		kind, _, err := classify(gs)
		if err != nil {
			return fmt.Errorf("step %d: %w", i, err)
		}
		if kind == kindStart && i != 0 {
			return fmt.Errorf("step %d: V is only valid as the first step", i)
		}
	}
	for _, c := range cursors {
		if c < 0 || c > len(steps) {
			return fmt.Errorf("entry cursor %d out of range", c)
		}
		if c == 0 && (len(steps) == 0 || classifyMust(steps[0]) != kindStart) {
			return fmt.Errorf("entry cursor 0 requires a V step")
		}
		marks := map[string]bool{}
		for i := c; i < len(steps); i++ {
			if has := steps[i].GetHas(); has != nil {
				for _, key := range hasKeys(has) {
					ns := tpath.GetNamespace(key)
					if ns != tpath.CURRENT && !marks[ns] {
						return fmt.Errorf("step %d: has refers to mark %q that is not available at entry cursor %d", i, ns, c)
					}
				}
			}
			if as := steps[i].GetAs(); as != "" {
				marks[as] = true
			}
		}
	}
	return nil
}

func classifyMust(gs *gripql.GraphStatement) stepKind {
	k, _, _ := classify(gs)
	return k
}

func hasKeys(e *gripql.HasExpression) []string {
	switch x := e.GetExpression().(type) {
	case *gripql.HasExpression_Condition:
		return []string{x.Condition.Key}
	case *gripql.HasExpression_And:
		out := []string{}
		for _, c := range x.And.Expressions {
			out = append(out, hasKeys(c)...)
		}
		return out
	case *gripql.HasExpression_Or:
		out := []string{}
		for _, c := range x.Or.Expressions {
			out = append(out, hasKeys(c)...)
		}
		return out
	case *gripql.HasExpression_Not:
		return hasKeys(x.Not)
	}
	return nil
}

// dataTypeAt returns the type of element a traveler holds when about to run steps[cursor].
func dataTypeAt(steps []*gripql.GraphStatement, cursor int) gdbi.DataType {
	t := gdbi.NoData
	for i := 0; i < cursor && i < len(steps); i++ {
		switch k, _, _ := classify(steps[i]); k {
		case kindStart, kindVertexMove:
			t = gdbi.VertexData
		case kindEdgeMove:
			t = gdbi.EdgeData
		}
	}
	return t
}

// compileSegment builds the processor chain that runs steps[cursor:] against db. A
// divert processor follows every step that produces vertices and hands travelers
// that left the node's prefix to the handoff function.
func compileSegment(db gdbi.GraphInterface, prefix string, steps []*gripql.GraphStatement, cursor int, handoff Handoff) ([]gdbi.Processor, error) {
	rest := steps[cursor:]
	stmts := make([]*gripql.GraphStatement, 0, len(rest))
	origin := make([]int, 0, len(rest)) // index in steps; -1 for rewritten statements

	// Only the statements before the first move are eligible for start optimizations,
	// which keeps the moves aligned with the original steps.
	firstMove := len(rest)
	for i, gs := range rest {
		if k := classifyMust(gs); k == kindVertexMove || k == kindEdgeMove {
			firstMove = i
			break
		}
	}
	if cursor == 0 {
		optimized := core.IndexStartOptimize(rest[:firstMove])
		for range optimized {
			origin = append(origin, -1)
		}
		stmts = append(stmts, optimized...)
	} else {
		for i := 0; i < firstMove; i++ {
			origin = append(origin, cursor+i)
		}
		stmts = append(stmts, rest[:firstMove]...)
	}
	for i := firstMove; i < len(rest); i++ {
		stmts = append(stmts, rest[i])
		origin = append(origin, cursor+i)
	}

	ps := gdbi.NewPipelineState(stmts, true)
	ps.LastType = dataTypeAt(steps, cursor)
	sc := core.NewStatementCompiler(db)

	procs := make([]gdbi.Processor, 0, len(stmts)*2)
	for i, gs := range stmts {
		ps.SetCurStatment(i)
		p, err := gdbi.StatementProcessor(sc, gs, db, ps)
		if err != nil {
			return nil, err
		}
		procs = append(procs, p)
		if origin[i] >= 0 && classifyMust(gs) == kindVertexMove {
			procs = append(procs, &divert{prefix: prefix, cursor: uint32(origin[i] + 1), handoff: handoff})
		}
	}
	return procs, nil
}

// divert removes travelers whose current vertex belongs to another source.
type divert struct {
	prefix  string
	cursor  uint32
	handoff Handoff
}

func (d *divert) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() || t.IsNull() || strings.HasPrefix(t.GetCurrentID(), d.prefix) {
				out <- t
				continue
			}
			d.handoff(t, d.cursor)
		}
	}()
	return ctx
}

// runProcessors chains processors and returns the output of the last one. If the
// chain is empty the input is returned unchanged.
func runProcessors(ctx context.Context, man gdbi.Manager, procs []gdbi.Processor, input <-chan gdbi.Traveler) <-chan gdbi.Traveler {
	if len(procs) == 0 {
		return input
	}
	const bufsize = 1000
	final := make(chan gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
		in := make(chan gdbi.Traveler, bufsize)
		ctx = procs[i].Process(ctx, man, in, out)
		out = in
	}
	go func() {
		defer close(out)
		for t := range input {
			out <- t
		}
	}()
	return final
}
