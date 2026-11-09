package gripper

import (
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

func errUnsupportedTraveler(t gdbi.Traveler) error {
	return fmt.Errorf("unsupported traveler type %T", t)
}

// federatedCompiler cuts a query into runs of steps that resource nodes can execute
// (segments) and steps that run on the central processor.
type federatedCompiler struct {
	g *FederatedGraph
}

func (g *FederatedGraph) Compiler() gdbi.Compiler {
	return federatedCompiler{g}
}

type pushKind int

const (
	pushNone pushKind = iota
	pushStart
	pushFilter
	pushMark
	pushVertexMove
	pushEdgeMove
)

// pushability classifies a statement. fanout is set for steps that need the
// sources holding inbound edges to be asked as well when applied to a vertex.
func pushability(gs *gripql.GraphStatement) (kind pushKind, fanout bool, labels []string) {
	switch s := gs.GetStatement().(type) {
	case *gripql.GraphStatement_V:
		return pushStart, false, nil
	case *gripql.GraphStatement_Has:
		for _, key := range hasExpressionKeys(s.Has) {
			if tpath.GetNamespace(key) != tpath.CURRENT {
				return pushNone, false, nil
			}
		}
		return pushFilter, false, nil
	case *gripql.GraphStatement_HasLabel, *gripql.GraphStatement_HasKey, *gripql.GraphStatement_HasId:
		return pushFilter, false, nil
	case *gripql.GraphStatement_As:
		return pushMark, false, nil
	case *gripql.GraphStatement_Out:
		return pushVertexMove, false, protoutil.AsStringList(s.Out)
	case *gripql.GraphStatement_OutE:
		return pushEdgeMove, false, protoutil.AsStringList(s.OutE)
	case *gripql.GraphStatement_In:
		return pushVertexMove, true, protoutil.AsStringList(s.In)
	case *gripql.GraphStatement_InE:
		return pushEdgeMove, true, protoutil.AsStringList(s.InE)
	case *gripql.GraphStatement_Both:
		return pushVertexMove, true, protoutil.AsStringList(s.Both)
	case *gripql.GraphStatement_BothE:
		return pushEdgeMove, true, protoutil.AsStringList(s.BothE)
	}
	return pushNone, false, nil
}

func hasExpressionKeys(e *gripql.HasExpression) []string {
	switch x := e.GetExpression().(type) {
	case *gripql.HasExpression_Condition:
		return []string{x.Condition.Key}
	case *gripql.HasExpression_And:
		out := []string{}
		for _, c := range x.And.Expressions {
			out = append(out, hasExpressionKeys(c)...)
		}
		return out
	case *gripql.HasExpression_Or:
		out := []string{}
		for _, c := range x.Or.Expressions {
			out = append(out, hasExpressionKeys(c)...)
		}
		return out
	case *gripql.HasExpression_Not:
		return hasExpressionKeys(x.Not)
	}
	return nil
}

// hasInboundLinks reports whether a vertex step with the labels may find edges stored
// in a source other than the vertex's owner.
func (g *FederatedGraph) hasInboundLinks(labels []string) bool {
	for _, s := range g.members() {
		for _, l := range s.Info.GetEdgeLinks() {
			if len(labels) == 0 || containsString(labels, l.Label) {
				return true
			}
		}
	}
	return false
}

// planSegment returns how many statements starting at i form a segment, or 0 if
// the statement at i has to run centrally. lastType is the element type before stmts[i].
func (g *FederatedGraph) planSegment(stmts []*gripql.GraphStatement, i int, lastType gdbi.DataType) int {
	cur := lastType
	n := 0
	moves := 0
	for j := i; j < len(stmts); j++ {
		kind, fanout, labels := pushability(stmts[j])
		if kind == pushNone || (kind == pushStart && j != 0) {
			break
		}
		if kind != pushStart && cur == gdbi.NoData {
			break
		}
		if fanout && cur == gdbi.VertexData && g.hasInboundLinks(labels) && j > i {
			break
		}
		switch kind {
		case pushStart, pushVertexMove:
			cur = gdbi.VertexData
		case pushEdgeMove:
			cur = gdbi.EdgeData
		}
		if kind == pushVertexMove || kind == pushEdgeMove || kind == pushStart {
			moves++
		}
		n++
	}
	if moves == 0 {
		// filters and marks alone are cheaper to run centrally
		return 0
	}
	return n
}

// newSegment builds the processor for a run of pushable statements entered with a traveler of type lastType.
func (g *FederatedGraph) newSegment(stmts []*gripql.GraphStatement, lastType gdbi.DataType) *segmentProc {
	p := &segmentProc{g: g}
	if _, ok := stmts[0].GetStatement().(*gripql.GraphStatement_V); ok {
		p.seed = true
		p.steps = append(p.steps, stmts...)
		p.first = 0
		if v := stmts[0].GetV(); v != nil {
			p.seedIDs = protoutil.AsStringList(v)
		}
		p.seedLabels = seedLabels(stmts)
	} else {
		p.steps = append(p.steps, vStatement())
		if lastType == gdbi.EdgeData {
			p.steps = append(p.steps, gripql.NewQuery().OutE().Statements[0])
		}
		p.first = uint32(len(p.steps))
		p.steps = append(p.steps, stmts...)
		_, fanout, labels := pushability(stmts[0])
		if fanout && lastType == gdbi.VertexData && g.hasInboundLinks(labels) {
			p.fanout = true
			p.fanoutLabels = labels
		}
	}
	// travelers resume after any step that produces vertices when handed off
	p.entries = []uint32{p.first}
	for j, gs := range p.steps {
		if k, _, _ := pushability(gs); k == pushVertexMove && j+1 < len(p.steps) {
			p.entries = append(p.entries, uint32(j+1))
		}
	}
	return p
}

// seedLabels returns the labels a seeding V().hasLabel(...) restricts vertices to.
func seedLabels(stmts []*gripql.GraphStatement) []string {
	for _, gs := range stmts[1:] {
		switch s := gs.GetStatement().(type) {
		case *gripql.GraphStatement_HasLabel:
			return protoutil.AsStringList(s.HasLabel)
		case *gripql.GraphStatement_Has, *gripql.GraphStatement_HasKey, *gripql.GraphStatement_HasId, *gripql.GraphStatement_As:
			continue
		default:
			return nil
		}
	}
	return nil
}

func (c federatedCompiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	g := c.g
	if opts != nil && opts.Extends != nil {
		// resumed pipelines start in the middle of a query; use plain lookups
		return core.NewCompiler(g, core.IndexStartOptimize).Compile(stmts, opts)
	}
	if len(stmts) == 0 {
		return core.NewCompiler(g).Compile(stmts, opts)
	}
	if err := core.Validate(stmts, opts); err != nil {
		return nil, fmt.Errorf("invalid statments: %s", err)
	}
	storeMarks := opts != nil && opts.StoreMarks
	ps := gdbi.NewPipelineState(stmts, storeMarks)
	sc := core.NewStatementCompiler(g)

	procs := make([]gdbi.Processor, 0, len(stmts))
	for i := 0; i < len(stmts); {
		n := g.planSegment(stmts, i, ps.LastType)
		if n == 0 {
			ps.SetCurStatment(i)
			p, err := gdbi.StatementProcessor(sc, stmts[i], g, ps)
			if err != nil {
				return nil, err
			}
			procs = append(procs, p)
			i++
			continue
		}
		seg := g.newSegment(stmts[i:i+n], ps.LastType)
		// compile the statements only to keep the pipeline state (types, marks) in step
		for j := i; j < i+n; j++ {
			ps.SetCurStatment(j)
			if _, err := gdbi.StatementProcessor(sc, stmts[j], g, ps); err != nil {
				return nil, err
			}
		}
		procs = append(procs, seg)
		i += n
	}
	return core.NewPipeline(g, procs, ps), nil
}
