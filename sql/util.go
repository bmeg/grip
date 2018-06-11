package sql

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	"github.com/hashicorp/go-multierror"
)

func rowDataToVertex(table string, schema *Schema, data map[string]interface{}, load bool) *aql.Vertex {
	v := &aql.Vertex{
		Gid:   fmt.Sprintf("%v:%v", table, data[schema.GetVertexGid(table)]),
		Label: schema.GetVertexLabel(table),
	}
	if load {
		v.Data = protoutil.AsStruct(data)
	}
	return v
}

func rowDataToEdge(table string, schema *Schema, data map[string]interface{}, load bool) *aql.Edge {
	e := &aql.Edge{
		Gid:   fmt.Sprintf("%v:%v", table, data[schema.GetEdgeGid(table)]),
		Label: schema.GetEdgeLabel(table),
		From:  fmt.Sprintf("%v", data[schema.GetEdgeFrom(table).SourceField]),
		To:    fmt.Sprintf("%v", data[schema.GetEdgeTo(table).SourceField]),
	}
	if load {
		e.Data = protoutil.AsStruct(data)
	}
	return e
}

type generatedEdgeID struct {
	Label     string
	FromTable string
	FromID    string
	ToTable   string
	ToID      string
}

func (geid generatedEdgeID) String() string {
	return fmt.Sprintf("generated:%s:%s:%s:%s:%s",
		url.QueryEscape(geid.Label),
		url.QueryEscape(geid.FromTable),
		url.QueryEscape(geid.FromID),
		url.QueryEscape(geid.ToTable),
		url.QueryEscape(geid.ToID),
	)
}

func parseGeneratedEdgeID(eid string) (*generatedEdgeID, error) {
	eid = strings.TrimPrefix(eid, "generated:")
	parts := strings.Split(eid, ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf("failed to parse edge id: unexpected content")
	}
	label, err := url.QueryUnescape(parts[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge id: %v", err)
	}
	fromTable, err := url.QueryUnescape(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge id: %v", err)
	}
	fromID, err := url.QueryUnescape(parts[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge id: %v", err)
	}
	toTable, err := url.QueryUnescape(parts[3])
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge id: %v", err)
	}
	toID, err := url.QueryUnescape(parts[4])
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge id: %v", err)
	}
	return &generatedEdgeID{label, fromTable, fromID, toTable, toID}, nil
}

func ValidateSchema(schema *Schema) error {
	var errs *multierror.Error
	vertices := make(map[string]interface{})
	for _, v := range schema.Vertices {
		if v.Table == "" {
			errs = multierror.Append(errs, fmt.Errorf("'Table' cannot be empty"))
		} else {
			vertices[v.Table] = nil
		}
		if v.GidField == "" {
			errs = multierror.Append(errs, fmt.Errorf("'GidField' cannot be empty"))
		}
		if v.Label == "" {
			errs = multierror.Append(errs, fmt.Errorf("'Label' cannot be empty"))
		}
	}
	vertexErrs := multierror.Prefix(errs, "vertex:")
	errs = nil
	for _, e := range schema.Edges {
		if e.Table == "" {
			if e.GidField != "" {
				errs = multierror.Append(errs, fmt.Errorf("'GidField' field must be empty if 'Table' is empty"))
			}
			if e.From == nil {
				errs = multierror.Append(errs, fmt.Errorf("'From' field cannot be nil if 'Table' is empty"))
			} else if e.From.SourceField != "" {
				errs = multierror.Append(errs, fmt.Errorf("'From.SourceField' field must be empty if 'Table' is empty"))
			}
			if e.To == nil {
				errs = multierror.Append(errs, fmt.Errorf("'To' field cannot be nil if 'Table' is empty"))
			} else if e.To.SourceField != "" {
				errs = multierror.Append(errs, fmt.Errorf("'To.SourceField' field must be empty if 'Table' is empty"))
			}
		} else {
			if _, ok := vertices[e.Table]; ok {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: previously declared as a vertex", e.Table))
			}
			if e.GidField == "" {
				errs = multierror.Append(errs, fmt.Errorf("'GidField' cannot be empty when referencing a table"))
			}
			if e.From != nil {
				if e.From.SourceField == "" {
					errs = multierror.Append(errs, fmt.Errorf("'From.SourceField' cannot be empty when referencing a table"))
				}
			}
			if e.To != nil {
				if e.To.SourceField == "" {
					errs = multierror.Append(errs, fmt.Errorf("'To.SourceField' cannot be empty when referencing a table"))
				}
			}
		}
		if e.Label == "" {
			errs = multierror.Append(errs, fmt.Errorf("'Label' cannot be empty"))
		}
		if e.From == nil {
			errs = multierror.Append(errs, fmt.Errorf("'From' cannot be nil"))
		} else if e.From.DestTable == "" {
			errs = multierror.Append(errs, fmt.Errorf("'From.DestTable' cannot be empty"))
		} else if _, ok := vertices[e.From.DestTable]; !ok {
			errs = multierror.Append(errs, fmt.Errorf("'From.DestTable' references an unknown vertex table: %s", e.From.DestTable))
		} else if e.From.DestField == "" {
			errs = multierror.Append(errs, fmt.Errorf("'From.DestField' cannot be empty"))
		}
		if e.To == nil {
			errs = multierror.Append(errs, fmt.Errorf("'To' cannot be nil"))
		} else if e.To.DestTable == "" {
			errs = multierror.Append(errs, fmt.Errorf("'To.DestTable' cannot be empty"))
		} else if _, ok := vertices[e.To.DestTable]; !ok {
			errs = multierror.Append(errs, fmt.Errorf("'To.DestTable' references an unknown vertex table: %s", e.To.DestTable))
		} else if e.To.DestField == "" {
			errs = multierror.Append(errs, fmt.Errorf("'To.DestField' cannot be empty"))
		}
	}
	edgeErrs := multierror.Prefix(errs, "edge:")
	return multierror.Append(vertexErrs, edgeErrs).ErrorOrNil()
}

func (s *Schema) GetVertexTables(label string) []string {
	tables := []string{}
	for _, v := range s.Vertices {
		if v.Label == label {
			tables = append(tables, v.Table)
		}
	}
	return tables
}

func (s *Schema) GetVertexGid(table string) string {
	for _, v := range s.Vertices {
		if v.Table == table {
			return v.GidField
		}
	}
	return ""
}

func (s *Schema) GetVertexLabel(table string) string {
	for _, v := range s.Vertices {
		if v.Table == table {
			return v.Label
		}
	}
	return ""
}

func (s *Schema) GetEdgeTables(label string) []string {
	tables := []string{}
	for _, v := range s.Edges {
		if v.Label == label {
			tables = append(tables, v.Table)
		}
	}
	return tables
}

func (s *Schema) GetEdgeGid(table string) string {
	for _, v := range s.Edges {
		if v.Table == table {
			return v.GidField
		}
	}
	return ""
}

func (s *Schema) GetEdgeLabel(table string) string {
	for _, v := range s.Edges {
		if v.Table == table {
			return v.Label
		}
	}
	return ""
}

func (s *Schema) GetEdgeFrom(table string) *ForeignKey {
	for _, v := range s.Edges {
		if v.Table == table {
			return v.From
		}
	}
	return nil
}

func (s *Schema) GetEdgeTo(table string) *ForeignKey {
	for _, v := range s.Edges {
		if v.Table == table {
			return v.To
		}
	}
	return nil
}

func (s *Schema) GetOutgoingEdges(table string, labels []string) []*Edge {
	out := []*Edge{}
	for _, v := range s.Edges {
		if v.From.DestTable == table {
			if len(labels) > 0 {
				for _, l := range labels {
					if v.Label == l {
						out = append(out, v)
					}
				}
			} else {
				out = append(out, v)
			}
		}
	}
	return out
}

func (s *Schema) GetIncomingEdges(table string, labels []string) []*Edge {
	out := []*Edge{}
	for _, v := range s.Edges {
		if v.To.DestTable == table {
			if len(labels) > 0 {
				for _, l := range labels {
					if v.Label == l {
						out = append(out, v)
					}
				}
			} else {
				out = append(out, v)
			}
		}
	}
	return out
}
