package gdbi

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/travelerpath"
)

func StatementProcessor(
	sc StatementCompiler,
	gs *gripql.GraphStatement,
	db GraphInterface,
	ps *State) (Processor, error) {

	switch stmt := gs.GetStatement().(type) {

	case *gripql.GraphStatement_V:
		if ps.LastType != NoData {
			return nil, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
		}
		o, err := sc.V(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_E:
		if ps.LastType != NoData {
			return nil, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
		}
		o, err := sc.E(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_In:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.In(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_InNull:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.InNull(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_Out:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.Out(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_OutNull:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.OutNull(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_Both:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.Both(stmt, ps)
		ps.LastType = VertexData
		return o, err

	case *gripql.GraphStatement_InE:
		if ps.LastType != VertexData {
			return nil, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		o, err := sc.InE(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_InENull:
		if ps.LastType != VertexData {
			return nil, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		o, err := sc.InENull(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_OutE:
		if ps.LastType != VertexData {
			return nil, fmt.Errorf(`"outEdgeNull" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		o, err := sc.OutE(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_OutENull:
		if ps.LastType != VertexData {
			return nil, fmt.Errorf(`"outEdgeNull" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		o, err := sc.OutENull(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_BothE:
		if ps.LastType != VertexData {
			return nil, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, ps.LastType.String())
		}
		o, err := sc.BothE(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_Has:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"Has" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.Has(stmt, ps)
		return o, err

	case *gripql.GraphStatement_HasLabel:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"HasLabel" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.HasLabel(stmt, ps)
		return o, err

	case *gripql.GraphStatement_HasKey:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"HasKey" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.HasKey(stmt, ps)
		return o, err

	case *gripql.GraphStatement_HasId:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"HasId" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.HasID(stmt, ps)
		ps.LastType = EdgeData
		return o, err

	case *gripql.GraphStatement_Limit:
		return sc.Limit(stmt, ps)

	case *gripql.GraphStatement_Skip:
		return sc.Skip(stmt, ps)

	case *gripql.GraphStatement_Range:
		return sc.Range(stmt, ps)

	case *gripql.GraphStatement_Count:
		o, err := sc.Count(stmt, ps)
		ps.LastType = CountData
		return o, err

	case *gripql.GraphStatement_Distinct:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		o, err := sc.Distinct(stmt, ps)
		return o, err

	case *gripql.GraphStatement_As:
		if ps.LastType == NoData {
			return nil, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
		}
		if stmt.As == "" {
			return nil, fmt.Errorf(`"mark" statement cannot have an empty name`)
		}
		if err := gripql.ValidateFieldName(stmt.As); err != nil {
			return nil, fmt.Errorf(`"mark" statement invalid; %v`, err)
		}
		if stmt.As == travelerpath.Current {
			return nil, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, travelerpath.Current)
		}
		ps.MarkTypes[stmt.As] = ps.LastType
		return sc.As(stmt, ps)

	case *gripql.GraphStatement_Set:
		return sc.Set(stmt, ps)

	case *gripql.GraphStatement_Increment:
		return sc.Increment(stmt, ps)

	case *gripql.GraphStatement_Mark:
		return sc.Mark(stmt, ps)

	case *gripql.GraphStatement_Jump:
		return sc.Jump(stmt, ps)

	case *gripql.GraphStatement_Select:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		out, err := sc.Select(stmt, ps)
		ps.LastType = ps.MarkTypes[stmt.Select.Marks[0]]
		return out, err

	case *gripql.GraphStatement_Render:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		out, err := sc.Render(stmt, ps)
		ps.LastType = RenderData
		return out, err

	case *gripql.GraphStatement_Path:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"path" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		out, err := sc.Path(stmt, ps)
		ps.LastType = PathData
		return out, err

	case *gripql.GraphStatement_Unwind:
		return sc.Unwind(stmt, ps)

	case *gripql.GraphStatement_Fields:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		return sc.Fields(stmt, ps)

	case *gripql.GraphStatement_Aggregate:
		if ps.LastType != VertexData && ps.LastType != EdgeData {
			return nil, fmt.Errorf(`"aggregate" statement is only valid for edge or vertex types not: %s`, ps.LastType.String())
		}
		out, err := sc.Aggregate(stmt, ps)
		ps.LastType = AggregationData
		return out, err

	default:

		return sc.Custom(gs, ps)
	}
}
