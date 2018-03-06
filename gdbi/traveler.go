package gdbi

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
)

// AddCurrent creates a new copy of the travel with new 'current' value
func (t Traveler) AddCurrent(r *DataElement) *Traveler {
	o := Traveler{marks: map[string]*DataElement{}}
	for k, v := range t.marks {
		o.marks[k] = v
	}
	o.current = r
	return &o
}

// HasLabeled checks to see if a results is stored in a travelers statemap
func (t Traveler) HasMark(label string) bool {
	_, ok := t.marks[label]
	return ok
}

// AddLabeled adds a result to travels state map using `label` as the name
func (t Traveler) AddMark(label string, r *DataElement) *Traveler {
	o := Traveler{marks: map[string]*DataElement{}}
	for k, v := range t.marks {
		o.marks[k] = v
	}
	o.marks[label] = r
	o.current = t.current
	return &o
}

// GetMark gets stored result in travels state using its label
func (t Traveler) GetMark(label string) *DataElement {
	lt := t.marks[label]
	return lt
}

// GetCurrent get current result value attached to the traveler
func (t Traveler) GetCurrent() *DataElement {
	return t.current
}

func (t Traveler) Convert(dataType DataType) *aql.ResultRow {
	switch dataType {
	case VertexData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Vertex{
					t.current.ToVertex(),
				},
			},
		}

	case EdgeData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Edge{
					t.current.ToEdge(),
				},
			},
		}

	case CountData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.Count),
				},
			},
		}

	case GroupCountData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.GroupCounts),
				},
			},
		}

	case RowData:
		res := &aql.ResultRow{}
		for _, r := range t.current.Row {
			elem := &aql.QueryResult{
				&aql.QueryResult_Vertex{
					r.ToVertex(), //BUG: lost the type by this point, guess its a vertex
				},
			}
			res.Row = append(res.Row, elem)
		}
		return res

	case ValueData:
		return &aql.ResultRow{
			Value: &aql.QueryResult{
				&aql.QueryResult_Data{
					protoutil.WrapValue(t.value),
				},
			},
		}

	default:
		panic(fmt.Errorf("unhandled data type %d", dataType))
	}
}

func (elem DataElement) ToVertex() *aql.Vertex {
	return &aql.Vertex{
		Gid:   elem.Id,
		Label: elem.Label,
		Data:  protoutil.AsStruct(elem.Data),
	}
}

func (elem DataElement) ToEdge() *aql.Edge {
	return &aql.Edge{
		Gid:   elem.Id,
		From:  elem.From,
		To:    elem.To,
		Label: elem.Label,
		Data:  protoutil.AsStruct(elem.Data),
	}
}

func (elem DataElement) ToDict() map[string]interface{} {
	out := map[string]interface{}{}
	if elem.Id != "" {
		out["gid"] = elem.Id
	}
	if elem.Label != "" {
		out["label"] = elem.Label
	}
	if elem.To != "" {
		out["to"] = elem.To
	}
	if elem.From != "" {
		out["from"] = elem.From
	}
	out["data"] = elem.Data
	return out
}
