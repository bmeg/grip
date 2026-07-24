package arango

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
)

const (
	transpilerCurrentField = "__current"
	transpilerPathField    = "__path"
	transpilerMarksField   = "__marks"
)

func TranslatePipeline(stmts []*gripql.GraphStatement, graphName string, includePath bool) (*ASTBase, error) {
	out := &ASTBase{}

	// Start with a root FOR loop over the Vertices collection.
	level := 0
	currentVar := fmt.Sprintf("v%d", level)
	currentType := "vertex"
	forLoop := &ForLoop{
		Variables:  []string{currentVar},
		Collection: "Vertices",
		GraphName:  graphName,
	}

	// Track the loop currently receiving body statements as the pipeline is translated.
	currentLoop := forLoop
	pathVar := ""
	marksVar := ""
	markVarLevel := 0
	if hasAsStatement(stmts) {
		marksVar = fmt.Sprintf("m%d", markVarLevel)
		currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: marksVar, Expr: "{}"})
	}
	if includePath {
		pathVar = fmt.Sprintf("p%d", level)
		currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: pathVar, Expr: fmt.Sprintf("[{vertex: %s._key}]", currentVar)})
	}
	for _, stmt := range stmts {
		switch stmt := stmt.GetStatement().(type) {
		case *gripql.GraphStatement_V:
			// Vertex selection resets the traversal to the Vertices collection.
			forLoop.Collection = "Vertices"
			currentType = "vertex"
			ids := make([]string, 0, len(stmt.V.GetValues()))
			for _, value := range stmt.V.GetValues() {
				if value != nil {
					ids = append(ids, value.GetStringValue())
				}
			}
			if len(ids) > 0 {
				var expr string
				if len(ids) == 1 {
					expr = fmt.Sprintf("%s._key == %q", currentVar, encodeDocumentKey(ids[0]))
				} else {
					quoted := make([]string, 0, len(ids))
					for _, id := range ids {
						quoted = append(quoted, fmt.Sprintf("%q", encodeDocumentKey(id)))
					}
					expr = fmt.Sprintf("%s._key IN [%s]", currentVar, strings.Join(quoted, ", "))
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
		case *gripql.GraphStatement_HasLabel:
			labels := make([]string, 0, len(stmt.HasLabel.GetValues()))
			for _, value := range stmt.HasLabel.GetValues() {
				if value != nil {
					labels = append(labels, value.GetStringValue())
				}
			}
			if len(labels) > 0 {
				var expr string
				if len(labels) == 1 {
					expr = fmt.Sprintf("%s._label == %q", currentVar, labels[0])
				} else {
					parts := make([]string, 0, len(labels))
					for _, label := range labels {
						parts = append(parts, fmt.Sprintf("%s._label == %q", currentVar, label))
					}
					expr = strings.Join(parts, " || ")
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
		case *gripql.GraphStatement_Out:
			// Out traversals become nested Arango FOR loops that walk one edge outward.
			if currentType != "vertex" {
				return nil, fmt.Errorf("out traversal only supported from vertex stream")
			}
			labels := make([]string, 0, len(stmt.Out.GetValues()))
			for _, value := range stmt.Out.GetValues() {
				if value != nil {
					labels = append(labels, value.GetStringValue())
				}
			}
			level++
			nextVar := fmt.Sprintf("v%d", level)
			nextEdge := fmt.Sprintf("e%d", level)
			nextLoop := &ForLoop{
				Variables: []string{nextVar, nextEdge},
				Range:     "1..1",
				Direction: "OUTBOUND",
				Source:    currentVar,
				GraphName: graphName,
			}
			currentLoop.Body.Children = append(currentLoop.Body.Children, nextLoop)
			currentLoop = nextLoop
			currentVar = nextVar
			currentType = "vertex"
			if len(labels) > 0 {
				var expr string
				if len(labels) == 1 {
					expr = fmt.Sprintf("%s._label == %q", nextEdge, labels[0])
				} else {
					parts := make([]string, 0, len(labels))
					for _, label := range labels {
						parts = append(parts, fmt.Sprintf("%s._label == %q", nextEdge, label))
					}
					expr = strings.Join(parts, " || ")
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
			if includePath {
				nextPathVar := fmt.Sprintf("p%d", level)
				currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextPathVar, Expr: fmt.Sprintf("APPEND(%s, [{vertex: %s._key}])", pathVar, nextVar)})
				pathVar = nextPathVar
			}
		case *gripql.GraphStatement_In:
			if currentType != "vertex" {
				return nil, fmt.Errorf("in traversal only supported from vertex stream")
			}
			labels := make([]string, 0, len(stmt.In.GetValues()))
			for _, value := range stmt.In.GetValues() {
				if value != nil {
					labels = append(labels, value.GetStringValue())
				}
			}
			level++
			nextVar := fmt.Sprintf("v%d", level)
			nextEdge := fmt.Sprintf("e%d", level)
			nextLoop := &ForLoop{
				Variables: []string{nextVar, nextEdge},
				Range:     "1..1",
				Direction: "INBOUND",
				Source:    currentVar,
				GraphName: graphName,
			}
			currentLoop.Body.Children = append(currentLoop.Body.Children, nextLoop)
			currentLoop = nextLoop
			currentVar = nextVar
			currentType = "vertex"
			if len(labels) > 0 {
				var expr string
				if len(labels) == 1 {
					expr = fmt.Sprintf("%s._label == %q", nextEdge, labels[0])
				} else {
					parts := make([]string, 0, len(labels))
					for _, label := range labels {
						parts = append(parts, fmt.Sprintf("%s._label == %q", nextEdge, label))
					}
					expr = strings.Join(parts, " || ")
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
			if includePath {
				nextPathVar := fmt.Sprintf("p%d", level)
				currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextPathVar, Expr: fmt.Sprintf("APPEND(%s, [{vertex: %s._key}])", pathVar, nextVar)})
				pathVar = nextPathVar
			}
		case *gripql.GraphStatement_Both:
			if currentType != "vertex" {
				return nil, fmt.Errorf("both traversal only supported from vertex stream")
			}
			labels := make([]string, 0, len(stmt.Both.GetValues()))
			for _, value := range stmt.Both.GetValues() {
				if value != nil {
					labels = append(labels, value.GetStringValue())
				}
			}
			level++
			nextVar := fmt.Sprintf("v%d", level)
			nextEdge := fmt.Sprintf("e%d", level)
			nextLoop := &ForLoop{
				Variables: []string{nextVar, nextEdge},
				Range:     "1..1",
				Direction: "ANY",
				Source:    currentVar,
				GraphName: graphName,
			}
			currentLoop.Body.Children = append(currentLoop.Body.Children, nextLoop)
			currentLoop = nextLoop
			currentVar = nextVar
			currentType = "vertex"
			if len(labels) > 0 {
				var expr string
				if len(labels) == 1 {
					expr = fmt.Sprintf("%s._label == %q", nextEdge, labels[0])
				} else {
					parts := make([]string, 0, len(labels))
					for _, label := range labels {
						parts = append(parts, fmt.Sprintf("%s._label == %q", nextEdge, label))
					}
					expr = strings.Join(parts, " || ")
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
			if includePath {
				nextPathVar := fmt.Sprintf("p%d", level)
				currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextPathVar, Expr: fmt.Sprintf("APPEND(%s, [{vertex: %s._key}])", pathVar, nextVar)})
				pathVar = nextPathVar
			}
		case *gripql.GraphStatement_As:
			if marksVar == "" {
				marksVar = fmt.Sprintf("m%d", markVarLevel)
				currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: marksVar, Expr: "{}"})
			}
			markVarLevel++
			nextMarksVar := fmt.Sprintf("m%d", markVarLevel)
			currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextMarksVar, Expr: fmt.Sprintf("MERGE(%s, {%q: %s})", marksVar, stmt.As, currentVar)})
			marksVar = nextMarksVar
		case *gripql.GraphStatement_Select:
			if marksVar == "" {
				return nil, fmt.Errorf("select statement requires at least one prior as statement")
			}
			level++
			nextVar := fmt.Sprintf("v%d", level)
			currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextVar, Expr: fmt.Sprintf("%s[%q]", marksVar, stmt.Select)})
			currentVar = nextVar
			currentType = "vertex"
			if includePath {
				nextPathVar := fmt.Sprintf("p%d", level)
				currentLoop.Body.Children = append(currentLoop.Body.Children, &LetStatement{Name: nextPathVar, Expr: fmt.Sprintf("APPEND(%s, [{vertex: %s._key}])", pathVar, nextVar)})
				pathVar = nextPathVar
			}
		case *gripql.GraphStatement_Limit:
			// Limit clauses are emitted as body statements on the current loop level.
			currentLoop.Body.Children = append(currentLoop.Body.Children, &LimitStatement{Limit: int(stmt.Limit)})
		case *gripql.GraphStatement_Skip:
			currentLoop.Body.Children = append(currentLoop.Body.Children, &OffsetLimitStatement{Offset: int(stmt.Skip), Count: 2147483647})
		case *gripql.GraphStatement_Range:
			r := stmt.Range
			offset := int(r.Start)
			if offset < 0 {
				offset = 0
			}
			if r.Stop < 0 {
				currentLoop.Body.Children = append(currentLoop.Body.Children, &OffsetLimitStatement{Offset: offset, Count: 2147483647})
				break
			}
			count := int(r.Stop - r.Start)
			if count < 0 {
				count = 0
			}
			currentLoop.Body.Children = append(currentLoop.Body.Children, &OffsetLimitStatement{Offset: offset, Count: count})
		case *gripql.GraphStatement_Sort:
			fields := make([]string, 0, len(stmt.Sort.GetFields()))
			for _, field := range stmt.Sort.GetFields() {
				if field != nil {
					expr := field.Field
					if field.Descending {
						expr += " DESC"
					} else {
						expr += " ASC"
					}
					fields = append(fields, expr)
				}
			}
			if len(fields) > 0 {
				currentLoop.Body.Children = append(currentLoop.Body.Children, &SortStatement{Expr: strings.Join(fields, ", ")})
			}
		default:
			return nil, fmt.Errorf("unsupported statement type %T", stmt)
		}
	}

	// Finish the current loop body with the active traversal variable.
	if includePath && marksVar != "" {
		currentLoop.Body.Children = append(currentLoop.Body.Children, &ReturnStatement{Variable: fmt.Sprintf("{%s: %s, %s: %s, %s: %s}", transpilerCurrentField, currentVar, transpilerPathField, pathVar, transpilerMarksField, marksVar)})
	} else if includePath {
		currentLoop.Body.Children = append(currentLoop.Body.Children, &ReturnStatement{Variable: fmt.Sprintf("{%s: %s, %s: %s}", transpilerCurrentField, currentVar, transpilerPathField, pathVar)})
	} else if marksVar != "" {
		currentLoop.Body.Children = append(currentLoop.Body.Children, &ReturnStatement{Variable: fmt.Sprintf("{%s: %s, %s: %s}", transpilerCurrentField, currentVar, transpilerMarksField, marksVar)})
	} else {
		currentLoop.Body.Children = append(currentLoop.Body.Children, &ReturnStatement{Variable: currentVar})
	}
	out.ForLoop = forLoop
	return out, nil
}

func hasAsStatement(stmts []*gripql.GraphStatement) bool {
	for _, gs := range stmts {
		if _, ok := gs.GetStatement().(*gripql.GraphStatement_As); ok {
			return true
		}
	}
	return false
}
