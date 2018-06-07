package sql

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	"github.com/hashicorp/go-multierror"
)

func RowDataToVertex(table string, schema *Schema, data map[string]interface{}, load bool) *aql.Vertex {
	key := fmt.Sprintf("%v:%v", table, data[schema.GetVertexGid(table)])
	v := &aql.Vertex{
		Gid:   key,
		Label: schema.GetVertexLabel(table),
	}
	if load {
		v.Data = protoutil.AsStruct(data)
	}
	return v
}

func generatedEdgeID(from, fromGid, to, toGid string) string {
	return fmt.Sprintf("generated:%s:%s:%s:%s",
		url.QueryEscape(from),
		url.QueryEscape(fromGid),
		url.QueryEscape(to),
		url.QueryEscape(toGid),
	)
}

func parseGeneratedEdgeID(eid string) (from, fromGid, to, toGid string, err error) {
	eid = strings.TrimPrefix(eid, "generated:")
	parts := strings.Split(eid, ":")
	if len(parts) != 4 {
		return "", "", "", "", fmt.Errorf("failed to parse edge id: unexpected content")
	}
	from, err = url.QueryUnescape(parts[0])
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to parse edge id: %v", err)
	}
	fromGid, err = url.QueryUnescape(parts[1])
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to parse edge id: %v", err)
	}
	to, err = url.QueryUnescape(parts[2])
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to parse edge id: %v", err)
	}
	toGid, err = url.QueryUnescape(parts[3])
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to parse edge id: %v", err)
	}
	return from, fromGid, to, toGid, nil
}

func RowDataToEdge(table string, schema *Schema, data map[string]interface{}, load bool) *aql.Edge {
	var gid, label, from, to string

	gidField := schema.GetEdgeGid(table)
	id := data[gidField]
	gid = fmt.Sprintf("%v:%v", table, id)

	label = schema.GetEdgeLabel(table)

	fromField := schema.GetEdgeFrom(table).SourceField
	from = fmt.Sprintf("%v", data[fromField])

	toField := schema.GetEdgeTo(table).SourceField
	to = fmt.Sprintf("%v", data[toField])

	edge := &aql.Edge{
		Gid:   gid,
		Label: label,
		From:  from,
		To:    to,
	}
	if load {
		edge.Data = protoutil.AsStruct(data)
	}
	return edge
}

func ValidateSchema(schema *Schema) error {
	var errs *multierror.Error
	for k, v := range schema.Vertices {
		if v.Table == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Table' field in cannot be empty", k))
		}
		if v.Gid == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Gid' field in cannot be empty", k))
		}
		if v.Label == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Label' field in cannot be empty", k))
		}
	}
	vertexErrs := multierror.Prefix(errs, "vertex:")
	errs = nil
	for k, e := range schema.Edges {
		if e.Table == "" {
			if e.Gid != "" {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Gid' field must be empty if 'Table' is empty", k))
			}
			if e.From == nil {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From' field cannot be nil if 'Table' is empty", k))
			} else if e.From.SourceField != "" {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From.SourceField' field must be empty if 'Table' is empty", k))
			}
			if e.To == nil {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To' field cannot be nil if 'Table' is empty", k))
			} else if e.To.SourceField != "" {
				errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To.SourceField' field must be empty if 'Table' is empty", k))
			}
		}
		if e.Table != "" && e.Gid == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Gid' field in cannot be empty", k))
		}
		if e.Label == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'Label' field in cannot be empty", k))
		}
		if e.From == nil {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From' field in cannot be nil", k))
		} else if e.Table != "" && e.From.SourceField == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From.SourceField' field in cannot be empty", k))
		} else if e.From.DestTable == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From.DestTable' field in cannot be empty", k))
		} else if e.From.DestField == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From.DestField' field in cannot be empty", k))
		} else if e.From.DestGid == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'From.DestGid' field in cannot be empty", k))
		}
		if e.To == nil {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To' field in cannot be nil", k))
		} else if e.Table != "" && e.To.SourceField == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To.SourceField' field in cannot be empty", k))
		} else if e.To.DestTable == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To.DestTable' field in cannot be empty", k))
		} else if e.To.DestField == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To.DestField' field in cannot be empty", k))
		} else if e.To.DestGid == "" {
			errs = multierror.Append(errs, fmt.Errorf("table: %s: 'To.DestGid' field in cannot be empty", k))
		}
	}
	edgeErrs := multierror.Prefix(errs, "edge:")
	return multierror.Append(vertexErrs, edgeErrs)
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
			return v.Gid
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
			return v.Gid
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

func (s *Schema) GetGeneratedEdgeLabel(from, to ForeignKey) string {
	for _, v := range s.Edges {
		if reflect.DeepEqual(v.From, from) && reflect.DeepEqual(v.To, to) {
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
