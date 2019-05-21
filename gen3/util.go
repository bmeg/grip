package gen3

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
)

type row struct {
	Gid   string
	Label string
	From  string
	To    string
	Data  []byte
}

func convertVertexRow(row *row, load bool) (*gripql.Vertex, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	v := &gripql.Vertex{
		Gid:   row.Gid,
		Label: row.Label,
		Data:  protoutil.AsStruct(props),
	}
	return v, nil
}

func convertEdgeRow(row *row, load bool) (*gripql.Edge, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	e := &gripql.Edge{
		Gid:   row.Gid,
		Label: row.Label,
		From:  row.From,
		To:    row.To,
		Data:  protoutil.AsStruct(props),
	}
	return e, nil
}

// read the schema files to determine the layout of the postgres database
func getGraphConfig(schemaDir string) (*graphConfig, error) {
	schemas, err := loadAllSchemas(schemaDir)
	if err != nil {
		return nil, err
	}
	g := &graphConfig{
		vertices: make(map[string]*vertexDef),
		edges:    make(map[string][]*edgeDef),
	}

  // initialize vertex objects
	for label, _ := range schemas {
		g.vertices[label] = &vertexDef{
			table: vertexTablename(label),
			out:   make(map[string][]*edgeDef),
			in:    make(map[string][]*edgeDef),
		}
  }

  // add edge info
  for srcLabel, data := range schemas {
		for _, link := range data.Links {
			eDef := &edgeDef{
				table:    edgeTablename(srcLabel, link.Label, link.TargetType),
				srcLabel: srcLabel,
				dstLabel: link.TargetType,
				backref:  false,
			}
			g.edges[link.Label] = append(g.edges[link.Label], eDef)
			bRef := &edgeDef{
				table:    edgeTablename(srcLabel, link.Label, link.TargetType),
				srcLabel: srcLabel,
				dstLabel: link.TargetType,
				backref:  true,
			}
			g.edges[link.Backref] = append(g.edges[link.Backref], bRef)
			g.vertices[srcLabel].out[link.Label] = append(g.vertices[srcLabel].out[link.Label], eDef)
			g.vertices[srcLabel].in[link.Backref] = append(g.vertices[srcLabel].in[link.Backref], bRef)
      g.vertices[link.TargetType].out[link.Backref] = append(g.vertices[link.TargetType].out[link.Backref], bRef)
      g.vertices[link.TargetType].in[link.Label] = append(g.vertices[link.TargetType].in[link.Label], eDef)
		}
	}
	return g, nil
}

func vertexTablename(label string) string {
	return fmt.Sprintf(
		"node_%s",
		strings.ReplaceAll(label, "_", ""),
	)
}

// https://github.com/uc-cdis/gdcdatamodel/blob/7aacbe2f383234b2ad4cb28418cb2f00dd2d24f7/gdcdatamodel/models/__init__.py#L370
func edgeTablename(srcLabel, label, dstLabel string) string {
	tablename := fmt.Sprintf(
		"edge_%s%s%s",
		strings.ReplaceAll(srcLabel, "_", ""),
		strings.ReplaceAll(label, "_", ""),
		strings.ReplaceAll(dstLabel, "_", ""),
	)

	if len(tablename) <= 40 {
		return tablename
	}

	truncSrc := []string{}
	truncLabel := []string{}
	truncDst := []string{}
	for _, x := range strings.Split(srcLabel, "_") {
		truncSrc = append(truncSrc, x[:2])
	}
	for _, x := range strings.Split(label, "_") {
		truncLabel = append(truncLabel, x[:2])
	}
	for _, x := range strings.Split(dstLabel, "_") {
		truncDst = append(truncDst, x[:2])
	}

	return fmt.Sprintf(
		"edge_%s_%s",
		fmt.Sprintf("%x", md5.Sum([]byte(tablename)))[:8],
		fmt.Sprintf(
			"%s%s%s", strings.Join(truncSrc, ""),
			strings.Join(truncLabel, ""),
			strings.Join(truncDst, ""),
		),
	)
}
