package arango

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
)

func TranslatePipeline(stmts []*gripql.GraphStatement, graphName string) (*ASTBase, error) {
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

	// Finish the current loop body with a RETURN of the active traversal variable.
	currentLoop.Body.Children = append(currentLoop.Body.Children, &ReturnStatement{Variable: currentVar})
	out.ForLoop = forLoop
	return out, nil
}
