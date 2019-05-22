package gen3

import (
	"crypto/md5"
	"fmt"
	"strings"
	//log "github.com/sirupsen/logrus"
)

type edgeDef struct {
	table    string
	label    string
	dstLabel string
	srcLabel string
	backref  bool
}

type vertexDef struct {
	table string
	label string
	out   map[string][]*edgeDef
	in    map[string][]*edgeDef
}

type graphConfig struct {
	// vertex label to vertexDef
	vertices map[string]*vertexDef
	// edge label to edgeDefs
	edges map[string][]*edgeDef
	// table name to vertex def
	vertexTables map[string]*vertexDef
	// table name to edge def
	edgeTables map[string]*edgeDef
}

// list vertex postgres tables
func (gc *graphConfig) listVertexTables() []string {
	tables := []string{}
	for k := range gc.vertexTables {
		tables = append(tables, k)
	}
	return tables
}

// list edge postgres tables
func (gc *graphConfig) listEdgeTables() []string {
	tables := []string{}
	for k := range gc.edgeTables {
		tables = append(tables, k)
	}
	return tables
}

// list all postgres tables
func (gc *graphConfig) listTables() []string {
	return append(gc.listVertexTables(), gc.listEdgeTables()...)
}

// get outgoing edgeDefs
func (gc *graphConfig) out(label string) map[string][]*edgeDef {
	if val, ok := gc.vertices[label]; ok {
		return val.out
	}
	return make(map[string][]*edgeDef)
}

// get incoming edgeDefs
func (gc *graphConfig) in(label string) map[string][]*edgeDef {
	if val, ok := gc.vertices[label]; ok {
		return val.in
	}
	return make(map[string][]*edgeDef)
}

// lookup label by tablename
func (gc *graphConfig) label(table string) string {
	label := ""
	if val, ok := gc.vertexTables[table]; ok {
		label = val.label
	} else if val, ok := gc.edgeTables[table]; ok {
		label = val.label
	}
	return label
}

// read the schema files to determine the layout of the postgres database
func getGraphConfig(schemaDir string) (*graphConfig, error) {
	schemas, err := loadAllSchemas(schemaDir)
	if err != nil {
		return nil, err
	}
	g := &graphConfig{
		vertices:     make(map[string]*vertexDef),
		vertexTables: make(map[string]*vertexDef),
		edges:        make(map[string][]*edgeDef),
		edgeTables:   make(map[string]*edgeDef),
	}

	// initialize vertex objects
	for label := range schemas {
		g.vertices[label] = &vertexDef{
			table: vertexTablename(label),
			label: label,
			out:   make(map[string][]*edgeDef),
			in:    make(map[string][]*edgeDef),
		}
	}

	// add edge info
	for srcLabel, data := range schemas {
		for _, link := range data.Links {
			eDef := &edgeDef{
				table:    edgeTablename(srcLabel, link.Label, link.TargetType),
				label:    link.Label,
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
	// fill in table lookups
	for _, v := range g.edges {
		for _, e := range v {
			g.edgeTables[e.table] = e
		}
	}
	for _, v := range g.vertices {
		g.vertexTables[v.table] = v
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
