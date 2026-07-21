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
			if len(labels) > 0 {
				var expr string
				if len(labels) == 1 {
					expr = fmt.Sprintf("%s.label == %q", nextEdge, labels[0])
				} else {
					parts := make([]string, 0, len(labels))
					for _, label := range labels {
						parts = append(parts, fmt.Sprintf("%s.label == %q", nextEdge, label))
					}
					expr = strings.Join(parts, " || ")
				}
				currentLoop.Body.Children = append(currentLoop.Body.Children, &FilterStatement{Expr: expr})
			}
		case *gripql.GraphStatement_Limit:
			// Limit clauses are emitted as body statements on the current loop level.
			currentLoop.Body.Children = append(currentLoop.Body.Children, &LimitStatement{Limit: int(stmt.Limit)})
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
