package grids

import (
	"fmt"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/protoutil"
)

// Compiler gets a compiler that will use the graph the execute the compiled query
func (ggraph *Graph) Compiler() gdbi.Compiler {
	return NewCompiler(ggraph)
}

type Compiler struct {
	graph *Graph
}

func SelectPath(stmts []*gripql.GraphStatement, path []int) []*gripql.GraphStatement {
	out := []*gripql.GraphStatement{}
	for _, p := range path {
		out = append(out, stmts[p])
	}
	return out
}

func NewCompiler(ggraph *Graph) gdbi.Compiler {
	return core.NewCompiler(ggraph, core.IndexStartOptimize)
	//return Compiler{graph: ggraph}
}

func (comp Compiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	fmt.Printf("Doing compile\n")
	if len(stmts) == 0 {
		return &core.DefaultPipeline{}, nil
	}

	if err := core.Validate(stmts, opts); err != nil {
		fmt.Printf("Failing validation\n")
		return &core.DefaultPipeline{}, fmt.Errorf("invalid statments: %s", err)
	}

	stmts = core.IndexStartOptimize(stmts)

	ps := gdbi.NewPipelineState(stmts)
	if opts != nil {
		ps.LastType = opts.PipelineExtension
		ps.MarkTypes = opts.ExtensionMarkTypes
	}
	fmt.Printf("GRIDS compile: %#v %#v\n", *ps, opts)

	procs := make([]gdbi.Processor, 0, len(stmts))

	optimizeOn := false

	fmt.Printf("Starting Grids Compiler: %s\n", stmts)
	for i := 0; i < len(stmts); i++ {
		gs := stmts[i]
		ps.SetCurStatment(i)
		if p, err := GetRawProcessor(comp.graph, ps, gs); err == nil && optimizeOn {
			procs = append(procs, p)
		} else {
			p, err := gdbi.StatementProcessor(gs, comp.graph, ps)
			if err != nil {
				fmt.Printf("Error %s at %d %#v", err, i, gs)
				return &core.DefaultPipeline{}, err
			}
			procs = append(procs, p)
		}
	}
	fmt.Printf("GRIDS: Pipeline: %#v\n", procs)
	return core.NewPipeline(comp.graph, procs, ps), nil
}

func GetRawProcessor(db *Graph, ps gdbi.PipelineState, stmt *gripql.GraphStatement) (gdbi.Processor, error) {
	switch stmt := stmt.GetStatement().(type) {
	case *gripql.GraphStatement_V:
		ids := protoutil.AsStringList(stmt.V)
		ps.SetLastType(gdbi.VertexData)
		return &PathVProc{db: db, ids: ids}, nil
	case *gripql.GraphStatement_In:
		if ps.GetLastType() == gdbi.VertexData {
			labels := protoutil.AsStringList(stmt.In)
			ps.SetLastType(gdbi.VertexData)
			return &PathInProc{db: db, labels: labels}, nil
		} else if ps.GetLastType() == gdbi.EdgeData {
			ps.SetLastType(gdbi.VertexData)
			return &PathInEdgeAdjProc{db: db}, nil
		}
	case *gripql.GraphStatement_Out:
		if ps.GetLastType() == gdbi.VertexData {
			labels := protoutil.AsStringList(stmt.Out)
			ps.SetLastType(gdbi.VertexData)
			return &PathOutProc{db: db, labels: labels}, nil
		} else if ps.GetLastType() == gdbi.EdgeData {
			ps.SetLastType(gdbi.VertexData)
			return &PathOutEdgeAdjProc{db: db}, nil
		}
	case *gripql.GraphStatement_InE:
		labels := protoutil.AsStringList(stmt.InE)
		ps.SetLastType(gdbi.EdgeData)
		return &PathInEProc{db: db, labels: labels}, nil
	case *gripql.GraphStatement_OutE:
		labels := protoutil.AsStringList(stmt.OutE)
		ps.SetLastType(gdbi.EdgeData)
		return &PathOutEProc{db: db, labels: labels}, nil
	case *gripql.GraphStatement_HasLabel:
		labels := protoutil.AsStringList(stmt.HasLabel)
		return &PathLabelProc{db: db, labels: labels}, nil
	}
	return nil, fmt.Errorf("unknown command: %T", stmt)
}
