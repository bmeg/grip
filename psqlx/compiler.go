package psqlx

import (
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"
)

type DelegatedRequest struct {
	Schema   string
	Function string
	Query    []byte
}

type delegatedCompiler struct {
	base     gdbi.Compiler
	conf     Config
	graph    string
	executor DelegatedExecutor
}

func newDelegatedCompiler(base gdbi.Compiler, conf Config, graph string, executor DelegatedExecutor) gdbi.Compiler {
	return &delegatedCompiler{base: base, conf: conf, graph: graph, executor: executor}
}

func (c *delegatedCompiler) Compile(stmts []*gripql.GraphStatement, opts *gdbi.CompileOptions) (gdbi.Pipeline, error) {
	if c.conf.DelegateTraversal {
		if err := validateDelegatedStatementSubset(stmts); err != nil {
			return nil, err
		}

		req, err := buildDelegatedRequest(c.conf, stmts)
		if err != nil {
			return nil, err
		}

		if c.conf.RequireDelegation {
			if c.executor == nil {
				return nil, fmt.Errorf(
					"psqlx: delegated traversal is enabled but no extension executor is configured for %s.%s",
					req.Schema,
					req.Function,
				)
			}
			return nil, fmt.Errorf(
				"psqlx: delegated traversal compiler path not yet integrated for graph %s; use delegated traversal execution path for %s.%s",
				c.graph,
				req.Schema,
				req.Function,
			)
		}

		// Interim behavior: validated delegated queries can still execute using
		// the existing compiler path until extension execution is wired.
	}
	return c.base.Compile(stmts, opts)
}

func buildDelegatedRequest(conf Config, stmts []*gripql.GraphStatement) (*DelegatedRequest, error) {
	enc := protojson.MarshalOptions{UseProtoNames: true}
	payload, err := enc.Marshal(&gripql.QuerySet{Query: stmts})
	if err != nil {
		return nil, fmt.Errorf("psqlx: failed to serialize delegated query: %v", err)
	}

	return &DelegatedRequest{
		Schema:   conf.ExtensionSchema,
		Function: conf.ExtensionFunction,
		Query:    payload,
	}, nil
}

func validateDelegatedStatementSubset(stmts []*gripql.GraphStatement) error {
	for i, gs := range stmts {
		switch gs.GetStatement().(type) {
		case *gripql.GraphStatement_V,
			*gripql.GraphStatement_In,
			*gripql.GraphStatement_Out,
			*gripql.GraphStatement_Both,
			*gripql.GraphStatement_InE,
			*gripql.GraphStatement_OutE,
			*gripql.GraphStatement_BothE,
			*gripql.GraphStatement_InNull,
			*gripql.GraphStatement_OutNull,
			*gripql.GraphStatement_InENull,
			*gripql.GraphStatement_OutENull,
			*gripql.GraphStatement_Has,
			*gripql.GraphStatement_HasLabel,
			*gripql.GraphStatement_HasKey,
			*gripql.GraphStatement_HasId,
			*gripql.GraphStatement_Limit,
			*gripql.GraphStatement_Skip,
			*gripql.GraphStatement_Range,
			*gripql.GraphStatement_Sort,
			*gripql.GraphStatement_Count,
			*gripql.GraphStatement_Distinct,
			*gripql.GraphStatement_Fields,
			*gripql.GraphStatement_Render:
			// Supported in delegated MVP.
		default:
			return fmt.Errorf("psqlx: delegated traversal does not support statement %q at index %d", statementName(gs), i)
		}
	}
	return nil
}

func statementName(gs *gripql.GraphStatement) string {
	switch gs.GetStatement().(type) {
	case *gripql.GraphStatement_V:
		return "v"
	case *gripql.GraphStatement_In:
		return "in"
	case *gripql.GraphStatement_Out:
		return "out"
	case *gripql.GraphStatement_Both:
		return "both"
	case *gripql.GraphStatement_InE:
		return "inE"
	case *gripql.GraphStatement_OutE:
		return "outE"
	case *gripql.GraphStatement_BothE:
		return "bothE"
	case *gripql.GraphStatement_InNull:
		return "inNull"
	case *gripql.GraphStatement_OutNull:
		return "outNull"
	case *gripql.GraphStatement_InENull:
		return "inENull"
	case *gripql.GraphStatement_OutENull:
		return "outENull"
	case *gripql.GraphStatement_Has:
		return "has"
	case *gripql.GraphStatement_HasLabel:
		return "hasLabel"
	case *gripql.GraphStatement_HasKey:
		return "hasKey"
	case *gripql.GraphStatement_HasId:
		return "hasId"
	case *gripql.GraphStatement_Limit:
		return "limit"
	case *gripql.GraphStatement_Skip:
		return "skip"
	case *gripql.GraphStatement_Range:
		return "range"
	case *gripql.GraphStatement_Sort:
		return "sort"
	case *gripql.GraphStatement_Count:
		return "count"
	case *gripql.GraphStatement_Distinct:
		return "distinct"
	case *gripql.GraphStatement_Fields:
		return "fields"
	case *gripql.GraphStatement_Render:
		return "render"
	case *gripql.GraphStatement_As:
		return "as"
	case *gripql.GraphStatement_Select:
		return "select"
	case *gripql.GraphStatement_Group:
		return "group"
	case *gripql.GraphStatement_Aggregate:
		return "aggregate"
	case *gripql.GraphStatement_Pivot:
		return "pivot"
	case *gripql.GraphStatement_Path:
		return "path"
	case *gripql.GraphStatement_Unwind:
		return "unwind"
	case *gripql.GraphStatement_Totype:
		return "totype"
	case *gripql.GraphStatement_Mark:
		return "mark"
	case *gripql.GraphStatement_Jump:
		return "jump"
	case *gripql.GraphStatement_Set:
		return "set"
	case *gripql.GraphStatement_Increment:
		return "increment"
	default:
		return "unknown"
	}
}
